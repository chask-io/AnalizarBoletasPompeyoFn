"""
Business logic for AnalizarBoletasPompeyoFn.

Pompeyo-owned receipt analyst for pipeline 81353. It analyzes PDF, DOCX, and
image receipts, then emits one deterministic batch artifact with attachment
inventory, OCR/extraction evidence, amount candidates, expense category
candidates, and confidence normalized to a documented 0..1 scale.

The handler.py file is infrastructure code and should NOT be modified.
"""

# MUST be first: Set HOME to /tmp for Lambda (litellm needs writable home directory)
import os
if not os.path.isdir(os.environ.get("HOME", "")):
    os.environ["HOME"] = "/tmp"

import base64
import io
import json
import logging
import math
import re
import hashlib
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from chask_foundation.backend.models import OrchestrationEvent
from backend.pompeyo_rules import (
    POMPEYO_EXTRACTION_RULES,
    normalize_financial_candidates,
)
from api.files_requests import files_api_manager
from api.pipeline_requests import pipeline_api_manager

logger = logging.getLogger()
logger.setLevel(logging.INFO)

LAMBDA_NAME = os.getenv("AWS_LAMBDA_FUNCTION_NAME", "AnalizarBoletasPompeyoFn")
ARTIFACT_SCHEMA_VERSION = "pompeyo.receipt_batch.v1"
PROVIDER_ALIASES = (
    "provider",
    "proveedor",
    "proveedor_razon_social",
    "razon_social_o_proveedor",
    "razon_social",
    "comercio",
)

# ~4 chars per token; 20,000 tokens ≈ 80,000 chars
MAX_CHARS_PDF_TEXT = 80_000
MAX_VISION_PAGES = 10   # Max pages to render for scanned PDF vision fallback
VISION_DPI = 200        # DPI for rendering PDF pages as images
MIN_PAGE_TEXT_CHARS = 30  # Pages with less extractable text are treated as scanned/image pages

ENABLE_ORIENTATION_NORMALIZATION = (
    os.getenv("ENABLE_ORIENTATION_NORMALIZATION", "true").lower()
    not in ("0", "false", "no", "off")
)
ORIENTATION_TEXTRACT_REGION = os.getenv("ORIENTATION_TEXTRACT_REGION", "us-east-1")
ORIENTATION_MAX_SIDE = int(os.getenv("ORIENTATION_MAX_SIDE", "1600"))
ORIENTATION_MIN_WORDS = int(os.getenv("ORIENTATION_MIN_WORDS", "4"))
ORIENTATION_MIN_WINNER_SHARE = float(os.getenv("ORIENTATION_MIN_WINNER_SHARE", "0.65"))
ORIENTATION_MIN_MARGIN = float(os.getenv("ORIENTATION_MIN_MARGIN", "0.25"))
ORIENTATION_WORD_CONFIDENCE = float(os.getenv("ORIENTATION_WORD_CONFIDENCE", "70"))

ENABLE_GEMINI_ENSEMBLE = (
    os.getenv("ENABLE_GEMINI_ENSEMBLE", "false").lower()
    not in ("0", "false", "no", "off")
)
ENSEMBLE_JUDGE_MODEL = os.getenv("ENSEMBLE_JUDGE_MODEL", "gpt-5.4-nano")
EXTRACTION_PRIMARY_MODEL = os.getenv("EXTRACTION_PRIMARY_MODEL", "gpt-5.4-mini")

_gemini_key_cache: Optional[str] = None
_gemini_key_fetched: bool = False

ENSEMBLE_JUDGE_SYSTEM_PROMPT = (
    "Eres un reconciliador de extracciones. Recibes dos extracciones independientes "
    "(Modelo A y Modelo B) del MISMO documento.\n\n"
    "PRINCIPIO CENTRAL: el grado de coincidencia entre Modelo A y Modelo B es tu principal "
    "medida de certeza. Si coinciden → alta confianza. Si DIFIEREN en un campo, eso es "
    "VARIABILIDAD = baja certeza: DEBES bajar la confianza de ESE campo (cuanto mayor la "
    "diferencia, más baja la confianza); NUNCA mantengas confianza alta en un campo donde los "
    "modelos discrepan. No 'resuelvas' la discrepancia eligiendo la opción más común/probable "
    "y subiendo la confianza — la discrepancia en sí ya es la señal de que no hay certeza.\n\n"
    "Para CADA campo en los JSON:\n"
    "- Si ambos modelos coinciden (normalizando formato/espacios/tildes): devuelve ese "
    "valor con confianza ALTA (90-99).\n"
    "- Si DIFIEREN en campos de IDENTIDAD (nombre, apellido, RUT, RUN, razón social, cédula): "
    "NO elijas la grafía más común o familiar; asigna confianza ≤80 y agrega "
    "\"nota_discrepancia\" con ambos valores originales DENTRO del dict del campo. "
    "En identidad NUNCA prefieras la grafía más común; la discrepancia entre modelos es señal "
    "de baja certeza → confianza ≤80.\n"
    "- Si DIFIEREN en campos NO-identidad: elige el valor más plausible, asigna confianza "
    "BAJA (<90, proporcional a la magnitud de la diferencia) y agrega \"nota_discrepancia\" "
    "(string con ambos valores originales) DENTRO del dict del campo.\n"
    "- Si solo un modelo aporta el campo: devuelve ese valor con confianza MEDIA (70-85).\n\n"
    "REGLAS ESTRICTAS:\n"
    "- CÉDULA CHILENA: puede mostrar FECHA DE EMISIÓN y FECHA DE VENCIMIENTO. "
    "fecha_vencimiento solo puede salir de una etiqueta explícita FECHA DE VENCIMIENTO/VENCIMIENTO. "
    "NUNCA reportes fecha_emision como fecha_vencimiento. Si los modelos confunden "
    "emisión con vencimiento o solo hay una fecha ambigua, baja la confianza de "
    "fecha_vencimiento y agrega nota_discrepancia.\n"
    "- CUOTAS CON CUOTÓN: al reconciliar cantidad_cuotas, si alguno de los modelos reporta "
    "presencia_cuoton=true o hay indicios de cuotón/VFG, prefiere el recuento TOTAL (el mayor) "
    "que incluye el cuotón. 48 regulares + 1 cuotón = 49; nunca reportes 48 en ese caso.\n"
    "- Devuelve EXACTAMENTE los mismos nombres de campo que recibas; no agregues ni "
    "elimines campos; no cambies la estructura.\n"
    "- NO penalices diferencias que son solo formato equivalente. Trata como coincidencia: "
    "mayúsculas/minúsculas; tildes/diacríticos; orden de tokens en nombres/razones sociales "
    "cuando los mismos tokens están presentes; fechas equivalentes escritas con distintos "
    "separadores u orden inequívoco; números/montos equivalentes con distintos separadores "
    "de miles/decimales. Cero, null, ausente y no aplica NO son equivalentes.\n"
    "- Por campo: {\"valor\": ..., \"confianza\": N} con 'nota_discrepancia' opcional.\n"
    "- Devuelve SOLO JSON válido con el schema:\n"
    "  {\"campos_extraidos\": {\"<campo>\": {\"valor\": ..., \"confianza\": N}}, "
    "\"extraction_confidence\": N}\n"
    "- No inventes campos ni valores."
)

CHILEAN_ID_DATE_RULES = (
    "- CÉDULA CHILENA: la cédula chilena tiene FECHA DE EMISIÓN y FECHA DE VENCIMIENTO. "
    "Extrae AMBAS como campos SEPARADOS: fecha_emision y fecha_vencimiento. "
    "fecha_vencimiento solo puede salir de una etiqueta explícita FECHA DE VENCIMIENTO/VENCIMIENTO. "
    "NUNCA reportes fecha_emision en fecha_vencimiento. Si solo se ve una fecha "
    "o la etiqueta es ambigua, usa fecha_vencimiento=null con confianza baja/0 y explica la ambigüedad.\n"
)

RECEIPT_ITEMS_RESPONSE_SCHEMA = (
    '- Cuando un archivo contiene una o más boletas/comprobantes, devuelve SIEMPRE un arreglo "receipts". '
    'Cada item representa una boleta detectada, no necesariamente una página. No asumas una boleta por página: '
    'si una boleta continúa en varias páginas, usa un solo item con page_range abarcando todas sus páginas.\n'
    '- Si no detectas ninguna boleta/comprobante, devuelve "receipts": [] y explica en observaciones.\n'
    '- Cada receipt debe incluir page_metadata con page_index (si aplica), page_range [inicio, fin] y group_label '
    'o discriminator cuando exista evidencia visual/textual para separar boletas.\n'
    '- Usa campos_extraidos dentro de cada receipt. Mantén compatibilidad con campos de archivo solo si el documento '
    'realmente contiene una única boleta.\n'
    '- Schema preferido:\n'
    '{\n'
    '  "file_name": "nombre breve del archivo",\n'
    '  "description": "descripción del archivo",\n'
    '  "receipts": [\n'
    '    {\n'
    '      "receipt_discriminator": "folio/proveedor/fecha/monto o etiqueta estable visible",\n'
    '      "page_metadata": {"page_index": 1, "page_range": [1, 2], "group_label": "boleta 1"},\n'
    '      "tipo_documento": "boleta | factura | comprobante | otro",\n'
    '      "description": "descripción de esta boleta",\n'
    '      "campos_extraidos": {\n'
    '        "<nombre_campo>": {"valor": "string o null", "confianza": 0, "pagina": 1}\n'
    '      },\n'
    '      "extraction_confidence": 0,\n'
    '      "ocr_text": "texto OCR relevante de esta boleta"\n'
    '    }\n'
    '  ],\n'
    '  "extraction_confidence": 0,\n'
    '  "observaciones": "notas de legibilidad o por qué receipts está vacío"\n'
    '}\n'
)

# pypdfium2 is NOT thread-safe. PDFs are processed concurrently (ThreadPoolExecutor),
# so all pdfium calls (open / text extraction / render) must be serialized with this
# lock to avoid native crashes (Runtime.ExitError). Only rendering is serialized; the
# slow vision LLM calls still run in parallel since they happen outside the lock.
_PDFIUM_LOCK = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# System Prompts
# ─────────────────────────────────────────────────────────────────────────────

PDF_SYSTEM_PROMPT = (
    'Eres **Receipt PDF Analyst**, un experto en analizar boletas, facturas y '
    'comprobantes PDF para rendición de gastos Pompeyo.\n\n'
    '**Instrucciones:**\n\n'
    '- NUNCA INVENTES DATOS. APÉGATE A LA INFORMACIÓN CONTENIDA EN ESTE PROMPT Y EN EL MENSAJE A CONTINUACIÓN.\n'
    '- Extrae datos concretos, específicos y verificables: proveedor/comercio, RUT si aparece, folio, fechas, '
    'montos neto/IVA/total, moneda, medio de pago, texto OCR relevante y posibles categorías de gasto.\n'
    '- NO escribas en ROMA, NO apruebes, NO rechaces y NO emitas decisiones de negocio. Solo entrega evidencia.\n'
    '- Si hay varias páginas o varios comprobantes en el archivo, preserva candidatos por página y marca ambigüedad.\n'
    '- El modelo NO debe redactar ni omitir nombres por privacidad: esto es validación de identidad operativa '
    'legítima. Devuelve el nombre completo tal como aparece en el documento.\n'
    "- TRANSCRIPCIÓN LITERAL DE NOMBRES PROPIOS: copia nombres, apellidos y razones sociales EXACTAMENTE "
    "carácter por carácter como aparecen, aunque la grafía sea inusual, rara o parezca un error de ortografía. "
    "NUNCA 'corrijas' un apellido a su forma más común o conocida (ej.: NO conviertas 'FEREIRA' en 'FERREIRA', "
    "ni 'PEÑALOZA' en 'PEÑALOSA', ni 'GONZALES' en 'GONZÁLEZ'). Tu tendencia a normalizar hacia nombres "
    "frecuentes es una FUENTE DE ERROR en validación de identidad. Si una letra de un nombre es genuinamente "
    "ilegible, baja la confianza de ese campo; jamás la reemplaces por la versión 'esperada'.\n"
    '- Cada campo extraído debe venir como objeto {"valor": <string|null>, "confianza": <entero 0-100>}.\n'
    '- Al asignar confianza, evalúa la CALIDAD de la fuente PDF y PENALIZA cuando las condiciones reducen la '
    'certeza de la lectura: rotación/orientación del documento; sombras, reflejos, brillo o flash sobre el texto; '
    'borrosidad, desenfoque o movimiento; recortes donde el campo esté parcialmente cortado o fuera de cuadro; '
    'baja resolución, pixelado, baja iluminación o bajo contraste. Para caracteres ambiguos, si un dígito o letra '
    'podría confundirse (6/8, 0/O, 1/I/l, 5/S, 2/Z, B/8, vocales con/sin tilde como v/y), baja la confianza de ESE '
    'campo proporcional a la ambigüedad. NO asumas el valor más probable con confianza alta.\n'
    '- Calibración de confianza: 95-100 = nítido, bien orientado, sin obstrucciones, cada carácter inequívoco; '
    '80-94 = legible pero con algún factor adverso (leve rotación, sombra parcial, un carácter algo ambiguo); '
    '60-79 = varios factores adversos o ambigüedad real en caracteres clave; <60 = difícil de leer, alto riesgo '
    'de error; 0 = no visible / no extraíble.\n'
    '- La confianza debe reflejar el RIESGO REAL de que el valor sea incorrecto. NUNCA inventes un valor para subir '
    'la confianza; si no se ve, valor=null y confianza=0. Si la calidad visual del documento es medium o bad, las '
    'confianzas individuales normalmente NO deberían ser >=95.\n'
    '- Incluye en campos_extraidos un par {valor, confianza} por CADA dato relevante que extraigas o que el prompt '
    'del nodo (analysis_prompt) te pida explícitamente. Las claves son los nombres de campo que correspondan al '
    'documento y al requerimiento; no asumas un conjunto fijo de campos.\n'
    '- Usa null y confianza 0 para campos relevantes que no estén visibles o no apliquen.\n'
    f"{CHILEAN_ID_DATE_RULES}"
    "- CUOTAS CON CUOTÓN (Compra Inteligente / VFG): cuando el crédito tiene un cuotón final / "
    "valor futuro garantizado (VFG) / una cuota final mayor que el resto, la CANTIDAD DE CUOTAS "
    "es el TOTAL incluyendo ese cuotón. Si el documento lista N cuotas regulares MÁS un cuotón/VFG "
    "(o el plazo es mayor que las cuotas regulares), reporta cantidad_cuotas = N+1. Ejemplo: 48 cuotas "
    "mensuales iguales + 1 cuotón final = reporta 49 (no 48). El cuotón es una cuota más. Cuando detectes "
    "cuotón/VFG, setea presencia_cuoton=true con confianza alta.\n"
    '- Presenta también los datos clave en key_data_table como tabla Markdown para facilitar lectura humana.\n\n'
    '**Formato de respuesta:**\n'
    'Devuelve solo JSON válido usando este contrato:\n'
    f'{RECEIPT_ITEMS_RESPONSE_SCHEMA}'
)

IMAGE_SYSTEM_PROMPT = (
    'Eres **Receipt Image Analyst**, un experto en observar imágenes de boletas, '
    'facturas y comprobantes para rendición de gastos Pompeyo.\n\n'
    '**Instrucciones:**\n\n'
    '- NUNCA INVENTES DATOS. APÉGATE A LA INFORMACIÓN CONTENIDA EN ESTE PROMPT Y EN EL MENSAJE A CONTINUACIÓN.\n'
    '- Identifica el tipo de documento de forma genérica según lo que observes, sin asumir categorías fijas.\n'
    '- Extrae de forma precisa proveedor/comercio, RUT si aparece, folio, fechas, montos neto/IVA/total, moneda, '
    'medio de pago, texto OCR relevante y posibles categorías de gasto.\n'
    '- NO escribas en ROMA, NO apruebes, NO rechaces y NO emitas decisiones de negocio. Solo entrega evidencia.\n'
    '- El modelo NO debe redactar ni omitir nombres por privacidad: esto es validación de identidad operativa '
    'legítima. Devuelve el nombre completo tal como aparece en el documento.\n'
    "- TRANSCRIPCIÓN LITERAL DE NOMBRES PROPIOS: copia nombres, apellidos y razones sociales EXACTAMENTE "
    "carácter por carácter como aparecen, aunque la grafía sea inusual, rara o parezca un error de ortografía. "
    "NUNCA 'corrijas' un apellido a su forma más común o conocida (ej.: NO conviertas 'FEREIRA' en 'FERREIRA', "
    "ni 'PEÑALOZA' en 'PEÑALOSA', ni 'GONZALES' en 'GONZÁLEZ'). Tu tendencia a normalizar hacia nombres "
    "frecuentes es una FUENTE DE ERROR en validación de identidad. Si una letra de un nombre es genuinamente "
    "ilegible, baja la confianza de ese campo; jamás la reemplaces por la versión 'esperada'.\n"
    '- Cada campo extraído debe venir como objeto {"valor": <string|null>, "confianza": <entero 0-100>}.\n'
    '- Al asignar confianza, evalúa la CALIDAD de la imagen y PENALIZA cuando las condiciones reducen la certeza '
    'de la lectura: rotación/orientación del documento, de lado o invertido; sombras, reflejos, brillo o flash '
    'sobre el texto; borrosidad, desenfoque o movimiento; recortes donde el campo esté parcialmente cortado o fuera '
    'de cuadro; baja resolución, pixelado, baja iluminación o bajo contraste. Para caracteres ambiguos, si un dígito '
    'o letra podría confundirse (6/8, 0/O, 1/I/l, 5/S, 2/Z, B/8, vocales con/sin tilde como v/y), baja la confianza '
    'de ESE campo proporcional a la ambigüedad. NO asumas el valor más probable con confianza alta.\n'
    '- Calibración de confianza: 95-100 = nítido, bien orientado, sin obstrucciones, cada carácter inequívoco; '
    '80-94 = legible pero con algún factor adverso (leve rotación, sombra parcial, un carácter algo ambiguo); '
    '60-79 = varios factores adversos o ambigüedad real en caracteres clave; <60 = difícil de leer, alto riesgo '
    'de error; 0 = no visible / no extraíble.\n'
    '- La confianza debe reflejar el RIESGO REAL de que el valor sea incorrecto. NUNCA inventes un valor para subir '
    'la confianza; si no se ve, valor=null y confianza=0. Si image_quality es medium o bad, las confianzas '
    'individuales normalmente NO deberían ser >=95.\n'
    '- Incluye en campos_extraidos un par {valor, confianza} por CADA dato relevante que extraigas o que el prompt '
    'del nodo (analysis_prompt) te pida explícitamente. Las claves son los nombres de campo que correspondan al '
    'documento y al requerimiento; no asumas un conjunto fijo de campos.\n'
    f"{CHILEAN_ID_DATE_RULES}"
    "- CUOTAS CON CUOTÓN (Compra Inteligente / VFG): cuando el crédito tiene un cuotón final / "
    "valor futuro garantizado (VFG) / una cuota final mayor que el resto, la CANTIDAD DE CUOTAS "
    "es el TOTAL incluyendo ese cuotón. Si el documento lista N cuotas regulares MÁS un cuotón/VFG "
    "(o el plazo es mayor que las cuotas regulares), reporta cantidad_cuotas = N+1. Ejemplo: 48 cuotas "
    "mensuales iguales + 1 cuotón final = reporta 49 (no 48). El cuotón es una cuota más. Cuando detectes "
    "cuotón/VFG, setea presencia_cuoton=true con confianza alta.\n"
    '- Incluye observaciones breves sobre legibilidad, orientación, recortes o borrosidad si afectan la extracción.\n\n'
    '**Formato de respuesta:**\n'
    'Devuelve solo JSON válido usando este contrato:\n'
    f'{RECEIPT_ITEMS_RESPONSE_SCHEMA}'
)

DOCX_SYSTEM_PROMPT = (
    'Eres **Receipt Document Analyst**, un experto en analizar documentos Word (DOCX) '
    'con boletas, facturas y comprobantes para rendición de gastos Pompeyo.\n\n'
    '**Instrucciones:**\n\n'
    '- NUNCA INVENTES DATOS. APÉGATE A LA INFORMACIÓN CONTENIDA EN ESTE PROMPT Y EN EL MENSAJE A CONTINUACIÓN.\n'
    '- Analiza texto e imágenes embebidas para identificar proveedor/comercio, RUT si aparece, folio, fechas, '
    'montos neto/IVA/total, moneda, medio de pago, texto OCR relevante y posibles categorías de gasto.\n'
    '- NO escribas en ROMA, NO apruebes, NO rechaces y NO emitas decisiones de negocio. Solo entrega evidencia.\n'
    '- El modelo NO debe redactar ni omitir nombres por privacidad: esto es validación de identidad operativa '
    'legítima. Devuelve el nombre completo tal como aparece en el documento.\n'
    "- TRANSCRIPCIÓN LITERAL DE NOMBRES PROPIOS: copia nombres, apellidos y razones sociales EXACTAMENTE "
    "carácter por carácter como aparecen, aunque la grafía sea inusual, rara o parezca un error de ortografía. "
    "NUNCA 'corrijas' un apellido a su forma más común o conocida (ej.: NO conviertas 'FEREIRA' en 'FERREIRA', "
    "ni 'PEÑALOZA' en 'PEÑALOSA', ni 'GONZALES' en 'GONZÁLEZ'). Tu tendencia a normalizar hacia nombres "
    "frecuentes es una FUENTE DE ERROR en validación de identidad. Si una letra de un nombre es genuinamente "
    "ilegible, baja la confianza de ese campo; jamás la reemplaces por la versión 'esperada'.\n"
    '- Cada campo extraído debe venir como objeto {"valor": <string|null>, "confianza": <entero 0-100>}.\n'
    '- Al asignar confianza, evalúa la CALIDAD de la fuente DOCX o de sus imágenes embebidas y PENALIZA cuando las '
    'condiciones reducen la certeza de la lectura: rotación/orientación del documento; sombras, reflejos, brillo o '
    'flash sobre el texto; borrosidad, desenfoque o movimiento; recortes donde el campo esté parcialmente cortado o '
    'fuera de cuadro; baja resolución, pixelado, baja iluminación o bajo contraste. Para caracteres ambiguos, si un '
    'dígito o letra podría confundirse (6/8, 0/O, 1/I/l, 5/S, 2/Z, B/8, vocales con/sin tilde como v/y), baja la '
    'confianza de ESE campo proporcional a la ambigüedad. NO asumas el valor más probable con confianza alta.\n'
    '- Calibración de confianza: 95-100 = nítido, bien orientado, sin obstrucciones, cada carácter inequívoco; '
    '80-94 = legible pero con algún factor adverso (leve rotación, sombra parcial, un carácter algo ambiguo); '
    '60-79 = varios factores adversos o ambigüedad real en caracteres clave; <60 = difícil de leer, alto riesgo '
    'de error; 0 = no visible / no extraíble.\n'
    '- La confianza debe reflejar el RIESGO REAL de que el valor sea incorrecto. NUNCA inventes un valor para subir '
    'la confianza; si no se ve, valor=null y confianza=0. Si la calidad visual del documento o de una imagen embebida '
    'es medium o bad, las confianzas individuales normalmente NO deberían ser >=95.\n'
    '- Incluye en campos_extraidos un par {valor, confianza} por CADA dato relevante que extraigas o que el prompt '
    'del nodo (analysis_prompt) te pida explícitamente. Las claves son los nombres de campo que correspondan al '
    'documento y al requerimiento; no asumas un conjunto fijo de campos.\n'
    '- Usa null y confianza 0 para campos relevantes que no estén visibles o no apliquen.\n'
    f"{CHILEAN_ID_DATE_RULES}"
    "- CUOTAS CON CUOTÓN (Compra Inteligente / VFG): cuando el crédito tiene un cuotón final / "
    "valor futuro garantizado (VFG) / una cuota final mayor que el resto, la CANTIDAD DE CUOTAS "
    "es el TOTAL incluyendo ese cuotón. Si el documento lista N cuotas regulares MÁS un cuotón/VFG "
    "(o el plazo es mayor que las cuotas regulares), reporta cantidad_cuotas = N+1. Ejemplo: 48 cuotas "
    "mensuales iguales + 1 cuotón final = reporta 49 (no 48). El cuotón es una cuota más. Cuando detectes "
    "cuotón/VFG, setea presencia_cuoton=true con confianza alta.\n"
    '- Presenta también los datos clave en key_data_table como tabla Markdown para facilitar lectura humana.\n\n'
    '**Formato de respuesta:**\n'
    'Devuelve solo JSON válido usando este contrato:\n'
    f'{RECEIPT_ITEMS_RESPONSE_SCHEMA}'
)

# DOCX extraction constants
ENSEMBLE_JUDGE_SYSTEM_PROMPT += "\n\n" + POMPEYO_EXTRACTION_RULES
PDF_SYSTEM_PROMPT += "\n\n" + POMPEYO_EXTRACTION_RULES
IMAGE_SYSTEM_PROMPT += "\n\n" + POMPEYO_EXTRACTION_RULES
DOCX_SYSTEM_PROMPT += "\n\n" + POMPEYO_EXTRACTION_RULES

MAX_DOCX_IMAGES = 10  # Max embedded images to analyze per DOCX
RASTER_EXTS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.tif'}


def _unwrap_response(response, error_label: str) -> dict:
    """
    Normalize API responses that may be raw dicts or requests.Response objects.
    Returns the parsed dict, or raises on non-200 HTTP responses.
    """
    if not hasattr(response, "status_code"):
        return response
    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"{error_label}: {response.status_code} {response.text}"
        )
    return response.json()


def _rut_dv_valido(valor: str) -> Optional[bool]:
    """Validate Chilean RUT/RUN check digit via módulo-11.

    Returns True if valid, False if invalid, None if the string does not
    look like a RUT (non-parseable formats are left untouched).
    """
    normalized = valor.replace(".", "").replace(" ", "").replace("-", "").upper()
    if len(normalized) < 2:
        return None
    cuerpo, dv_provisto = normalized[:-1], normalized[-1]
    if not cuerpo.isdigit() or len(cuerpo) < 7 or len(cuerpo) > 8:
        return None
    if dv_provisto not in "0123456789K":
        return None
    pesos = [2, 3, 4, 5, 6, 7]
    suma = sum(int(d) * pesos[i % 6] for i, d in enumerate(reversed(cuerpo)))
    resto = suma % 11
    dv_calc = 11 - resto
    if dv_calc == 11:
        dv_esperado = "0"
    elif dv_calc == 10:
        dv_esperado = "K"
    else:
        dv_esperado = str(dv_calc)
    return dv_provisto == dv_esperado


class FunctionBackend:
    """
    Combined PDF + Image analyst backend.

    Processes PDF documents and images from the orchestration session
    using LLMClient for centralized logging and token tracking.
    """

    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event
        self._source_bytes_cache: Dict[str, bytes] = {}
        logger.info(
            f"Initialized FunctionBackend for org: "
            f"{orchestration_event.organization.organization_id}"
        )

    def process_request(self) -> str:
        tool_args = self._extract_tool_args()

        analysis_prompt = tool_args.get("analysis_prompt")
        if not analysis_prompt:
            raise ValueError("Missing required parameter: analysis_prompt")

        file_uuids = tool_args.get("file_uuids")
        node_id = tool_args.get("node_id", "")
        category_catalog_snapshot = self._parse_category_catalog_snapshot(
            tool_args.get("category_catalog_snapshot")
            or (self.orchestration_event.extra_params or {}).get("category_catalog_snapshot")
        )

        extra_params = self.orchestration_event.extra_params or {}
        logger.info(
            f"file_uuids={file_uuids}, node_id={node_id}, "
            f"prompt_len={len(analysis_prompt)}, "
            f"extra_params_keys={list(extra_params.keys())}, "
            f"test_file_uuids={extra_params.get('test_file_uuids')}, "
            f"attachments={extra_params.get('attachments')}"
        )

        # 1. Get all session files
        session_files = self._get_session_files()
        logger.info(f"Found {len(session_files)} files in session")

        # 2. Resolve and validate file selection
        pdf_files, image_files, docx_files, skipped = self._resolve_file_selection(
            file_uuids, session_files
        )

        if not pdf_files and not image_files and not docx_files:
            report = self._build_processing_report(pdf_files, image_files, docx_files, skipped)
            return self._build_final_response(
                {}, pdf_files, image_files, docx_files, skipped, report,
                category_catalog_snapshot,
            )

        logger.info(
            f"Processing {len(pdf_files)} PDFs + {len(image_files)} images + "
            f"{len(docx_files)} DOCX, skipping {len(skipped)} files"
        )

        # 3. Enrich prompt with node context
        full_prompt = analysis_prompt.strip()
        if node_id:
            node_context = self._get_node_context(node_id)
            if node_context:
                full_prompt += "\n\nContexto del requerimiento:\n" + node_context
                logger.info(f"Added node context (node_id: {node_id})")

        # 4. Initialize LLM client and process files
        from chask_foundation.llm import LLMClient

        llm_client = None
        try:
            llm_client = LLMClient(
                access_token=self.orchestration_event.access_token,
                organization_id=self.orchestration_event.organization.organization_id,
                orchestration_session_uuid=self.orchestration_event.orchestration_session_uuid,
                internal_orchestration_session_uuid=self.orchestration_event.internal_orchestration_session_uuid,
                orchestration_event_uuid=str(self.orchestration_event.event_id),
                default_model=self._get_model(),
                openai_api_key=self._get_openai_api_key(),
            )

            # 5. Process all files in parallel
            results = self._process_files_parallel(
                llm_client, pdf_files, image_files, docx_files, full_prompt
            )

            # 6. Build final response
            report = self._build_processing_report(pdf_files, image_files, docx_files, skipped)
            return self._build_final_response(
                results, pdf_files, image_files, docx_files, skipped, report,
                category_catalog_snapshot,
            )

        finally:
            if llm_client:
                llm_client.shutdown()

    # ─────────────────────────────────────────────────────────────────────────
    # File resolution
    # ─────────────────────────────────────────────────────────────────────────

    def _get_session_files(self) -> List[dict]:
        try:
            response = files_api_manager.call(
                "get_all_files_for_session",
                orchestration_session_uuid=self.orchestration_event.orchestration_session_uuid,
                internal_orchestration_session_uuid=self.orchestration_event.internal_orchestration_session_uuid,
                access_token=self.orchestration_event.access_token,
                organization_id=self.orchestration_event.organization.organization_id,
            )
            files_data = _unwrap_response(response, "Failed to get session files")
            return files_data.get("files", [])
        except Exception as e:
            logger.error(f"Exception getting session files: {e}")
            return []

    def _is_pdf(self, file_record: dict) -> bool:
        mime = file_record.get("mime_type") or file_record.get("content_type", "")
        if mime == "application/pdf" or mime.endswith("/pdf"):
            return True
        if mime in ("application/octet-stream", ""):
            name = (file_record.get("file_name") or "").lower()
            return name.endswith(".pdf")
        return False

    def _is_image(self, file_record: dict) -> bool:
        mime = file_record.get("mime_type") or file_record.get("content_type", "")
        if mime.startswith("image/"):
            return True
        if mime in ("application/octet-stream", ""):
            name = (file_record.get("file_name") or "").lower()
            return any(name.endswith(ext) for ext in (
                ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".tif", ".webp",
            ))
        return False

    def _is_docx(self, file_record: dict) -> bool:
        mime = file_record.get("mime_type") or file_record.get("content_type", "")
        if mime in (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/docx",
        ):
            return True
        if mime in ("application/octet-stream", ""):
            name = (file_record.get("file_name") or "").lower()
            return name.endswith(".docx")
        return False

    def _is_video(self, file_record: dict) -> bool:
        mime = file_record.get("mime_type") or file_record.get("content_type", "")
        if mime.startswith("video/"):
            return True
        if mime in ("application/octet-stream", ""):
            name = (file_record.get("file_name") or "").lower()
            return any(name.endswith(ext) for ext in (".mp4", ".mov", ".avi", ".webm"))
        return False

    def _raise_uuid_not_found(self, uid: str, session_files: List[dict]) -> None:
        available = "\n".join(
            f"  - {f.get('file_name', '?')} (UUID: {f['file_uuid']}, "
            f"tipo: {f.get('mime_type', '?')})"
            for f in session_files
        )
        raise ValueError(
            f"Archivo con UUID '{uid}' no encontrado en la sesión.\n\n"
            f"Archivos disponibles:\n{available}"
        )

    def _resolve_file_selection(
        self,
        file_uuids: Optional[Union[str, List[str]]],
        session_files: List[dict],
    ) -> Tuple[List[dict], List[dict], List[dict], List[Tuple[str, str]]]:
        """
        Resolve file selection into (pdf_files, image_files, docx_files, skipped).

        Args:
            file_uuids: "all", None/empty, or list of UUIDs
            session_files: All files in the session

        Returns:
            (pdf_files, image_files, docx_files, skipped) where skipped is
            list of (filename, reason) tuples
        """
        if not session_files:
            return [], [], [], [("N/A", "No hay archivos en la sesión")]

        files_by_uuid = {f["file_uuid"]: f for f in session_files}
        pdfs = []
        images = []
        docx = []
        skipped = []

        # Determine candidate files
        process_all = (
            file_uuids is None
            or file_uuids == ""
            or (isinstance(file_uuids, str) and file_uuids.lower() == "all")
            or (isinstance(file_uuids, list) and len(file_uuids) == 0)
        )

        if process_all:
            candidates = session_files
        elif isinstance(file_uuids, list):
            candidates = []
            for uid in file_uuids:
                rec = files_by_uuid.get(uid)
                if rec is None:
                    self._raise_uuid_not_found(uid, session_files)
                candidates.append(rec)
        elif isinstance(file_uuids, str):
            # Single UUID string (not "all")
            rec = files_by_uuid.get(file_uuids)
            if rec is None:
                self._raise_uuid_not_found(file_uuids, session_files)
            candidates = [rec]
        else:
            candidates = session_files

        # Classify files
        for rec in candidates:
            if self._is_pdf(rec):
                pdfs.append(rec)
            elif self._is_docx(rec):
                docx.append(rec)
            elif self._is_image(rec):
                images.append(rec)
            elif self._is_video(rec):
                skipped.append((rec.get("file_name", "?"), "archivo de video (omitido)"))
            else:
                mime = rec.get("mime_type") or rec.get("content_type", "desconocido")
                name = rec.get("file_name", "?")
                if not process_all:
                    raise ValueError(
                        f"El archivo '{name}' es de tipo '{mime}', "
                        f"no es un tipo soportado (PDF, DOCX o imagen)."
                    )
                skipped.append((name, f"tipo no soportado ({mime})"))

        return pdfs, images, docx, skipped

    # ─────────────────────────────────────────────────────────────────────────
    # Node context
    # ─────────────────────────────────────────────────────────────────────────

    def _get_node_context(self, node_id: str) -> str:
        try:
            response = pipeline_api_manager.call(
                "get_pipeline",
                pipeline_id=self.orchestration_event.pipeline_id,
                access_token=self.orchestration_event.access_token,
                organization_id=self.orchestration_event.organization.organization_id,
            )
            pipeline_data = _unwrap_response(response, "Failed to get pipeline")

            for node in pipeline_data.get("nodes", []):
                if str(node.get("id")) == str(node_id):
                    return node.get("description", "")

            logger.warning(f"Node {node_id} not found in pipeline")
            return ""

        except Exception as e:
            logger.error(f"Exception getting node context: {e}")
            return ""

    # ─────────────────────────────────────────────────────────────────────────
    # Parallel file processing
    # ─────────────────────────────────────────────────────────────────────────

    def _process_files_parallel(
        self,
        llm_client: Any,
        pdf_files: List[dict],
        image_files: List[dict],
        docx_files: List[dict],
        prompt: str,
    ) -> Dict[str, str]:
        all_tasks: List[Tuple[str, dict]] = (
            [("pdf", f) for f in pdf_files]
            + [("image", f) for f in image_files]
            + [("docx", f) for f in docx_files]
        )

        results: Dict[str, str] = {}

        with ThreadPoolExecutor(max_workers=min(len(all_tasks), 5)) as executor:
            futures = {}
            for file_type, file_record in all_tasks:
                if file_type == "pdf":
                    future = executor.submit(
                        self._process_single_pdf, llm_client, file_record, prompt
                    )
                elif file_type == "docx":
                    future = executor.submit(
                        self._process_single_docx, llm_client, file_record, prompt
                    )
                else:
                    future = executor.submit(
                        self._process_single_image, llm_client, file_record, prompt
                    )
                futures[future] = (file_record["file_uuid"], file_record.get("file_name", "?"))

            for future in as_completed(futures):
                file_uuid, file_name = futures[future]
                try:
                    results[file_uuid] = future.result()
                    logger.info(f"Completed: {file_name}")
                except Exception as e:
                    logger.error(f"Failed processing {file_name}: {e}")
                    results[file_uuid] = f"Error procesando archivo: {e}"

        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Vision image preprocessing
    # ─────────────────────────────────────────────────────────────────────────

    def _orient_upright(self, pil_image: Any, source_label: str) -> Any:
        """Return a visually upright copy when Textract geometry is confident."""
        from PIL import ImageOps

        image = ImageOps.exif_transpose(pil_image)
        if not ENABLE_ORIENTATION_NORMALIZATION:
            logger.info(f"orientation_status=disabled source={source_label}")
            return image

        try:
            degrees, confident = self._detect_orientation_textract(image)
        except Exception as e:
            logger.warning(
                f"orientation_status=error source={source_label} error={e}; "
                "using original orientation"
            )
            return image

        if not confident:
            logger.info(
                f"orientation_status=no_quorum source={source_label} detected={degrees}; "
                "using original orientation"
            )
            return image

        rotation = (360 - degrees) % 360
        if rotation == 0:
            logger.info(f"orientation_status=upright source={source_label} detected=0")
            return image

        logger.info(
            f"orientation_status=rotated source={source_label} detected={degrees} "
            f"applied={rotation}"
        )
        return image.rotate(rotation, expand=True)

    def _detect_orientation_textract(self, pil_image: Any) -> Tuple[int, bool]:
        """Detect text direction from Textract WORD polygons."""
        import boto3

        image_bytes = self._prepare_orientation_image_bytes(pil_image)
        response = boto3.client(
            "textract",
            region_name=ORIENTATION_TEXTRACT_REGION,
        ).detect_document_text(Document={"Bytes": image_bytes})

        scores = {0: 0.0, 90: 0.0, 180: 0.0, 270: 0.0}
        word_count = 0
        for block in response.get("Blocks", []):
            if block.get("BlockType") != "WORD":
                continue
            confidence = float(block.get("Confidence") or 0)
            text = block.get("Text") or ""
            if (
                confidence < ORIENTATION_WORD_CONFIDENCE
                or not any(ch.isalnum() for ch in text)
            ):
                continue

            angle = self._word_polygon_angle(block)
            if angle is None:
                continue

            scores[self._snap_orientation_angle(angle)] += confidence
            word_count += 1

        if word_count < ORIENTATION_MIN_WORDS:
            logger.info(
                f"orientation_textract words={word_count} scores={scores} quorum=false"
            )
            return 0, False

        ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        winner, winner_score = ordered[0]
        total_score = sum(scores.values())
        second_score = ordered[1][1] if len(ordered) > 1 else 0.0
        winner_share = winner_score / total_score if total_score else 0.0
        margin = (winner_score - second_score) / total_score if total_score else 0.0
        confident = (
            winner_share >= ORIENTATION_MIN_WINNER_SHARE
            and margin >= ORIENTATION_MIN_MARGIN
        )

        logger.info(
            "orientation_textract words=%s winner=%s share=%.2f margin=%.2f "
            "quorum=%s scores=%s",
            word_count,
            winner,
            winner_share,
            margin,
            confident,
            scores,
        )
        return winner, confident

    def _prepare_orientation_image_bytes(self, pil_image: Any) -> bytes:
        image = pil_image.copy()
        image.thumbnail((ORIENTATION_MAX_SIDE, ORIENTATION_MAX_SIDE))
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")
        buf = io.BytesIO()
        image.save(buf, format="JPEG", quality=90, optimize=True)
        return buf.getvalue()

    def _word_polygon_angle(self, block: dict) -> Optional[float]:
        polygon = (block.get("Geometry") or {}).get("Polygon") or []
        if len(polygon) < 2:
            return None

        p0, p1 = polygon[0], polygon[1]
        dx = float(p1.get("X", 0)) - float(p0.get("X", 0))
        dy = float(p1.get("Y", 0)) - float(p0.get("Y", 0))
        if abs(dx) < 1e-9 and abs(dy) < 1e-9:
            return None
        return math.degrees(math.atan2(dy, dx)) % 360

    def _snap_orientation_angle(self, angle: float) -> int:
        return int(round(angle / 90.0) * 90) % 360

    def _pil_image_part(self, pil_image: Any) -> dict:
        image = pil_image
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")
        buf = io.BytesIO()
        image.save(buf, format="PNG")
        b64_data = base64.b64encode(buf.getvalue()).decode("ascii")
        return {
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64_data}", "detail": "high"},
        }

    def _embedded_image_part(self, image_data: dict) -> dict:
        from PIL import Image

        try:
            pil_image = Image.open(io.BytesIO(image_data["bytes"]))
            pil_image = self._orient_upright(pil_image, image_data["name"])
            return self._pil_image_part(pil_image)
        except Exception as e:
            logger.warning(
                f"Embedded image orientation failed for {image_data.get('name')} ({e}); "
                "using original image bytes"
            )
            b64_data = base64.b64encode(image_data["bytes"]).decode("ascii")
            return {
                "type": "image_url",
                "image_url": {
                    "url": f"data:{image_data['mime']};base64,{b64_data}",
                    "detail": "high",
                },
            }

    # ─────────────────────────────────────────────────────────────────────────
    # PDF processing
    # ─────────────────────────────────────────────────────────────────────────

    def _process_single_pdf(self, llm_client: Any, file_record: dict, prompt: str) -> str:
        file_uuid = file_record["file_uuid"]
        file_name = file_record.get("file_name", file_uuid)

        logger.info(f"Processing PDF: {file_name} ({file_uuid})")

        # Fetch PDF text via API
        pdf_response = files_api_manager.call(
            "read_single_pdf_text",
            attachment_uuid=file_uuid,
            origin="pdf_analyst",
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
        )

        if hasattr(pdf_response, "status_code"):
            if pdf_response.status_code != 200:
                return f"Error leyendo PDF ({pdf_response.status_code}): {file_name}"
            pdf_data = pdf_response.json()
        else:
            pdf_data = pdf_response

        pdf_text = (pdf_data.get("text", "") or "").strip()

        # If no extractable text (or too short to be useful), fall back to full vision
        if len(pdf_text) < 50:
            logger.info(
                f"Insufficient text in {file_name} ({len(pdf_text)} chars), "
                f"falling back to vision analysis"
            )
            return self._process_pdf_as_image(llm_client, file_record, prompt)

        logger.info(f"PDF text for {file_name}: {len(pdf_text)} chars, preview: {pdf_text[:100]!r}")

        # Always render all pages and send text+images to vision.
        # PDFs with thin text layers (e.g. "Print to PDF") carry key data in embedded
        # images that text extraction misses entirely. Rendering every page ensures
        # no content is silently dropped. Falls through to text-only only if render fails.
        total_pages, image_page_indices, pdf_bytes = self._analyze_pdf_pages(file_record)
        if pdf_bytes is not None:
            all_page_indices = list(range(min(total_pages, MAX_VISION_PAGES)))
            logger.info(
                f"PDF {file_name}: rendering all {len(all_page_indices)}/{total_pages} page(s) "
                f"for vision ({len(image_page_indices)} scanned page(s) detected: "
                f"{[i + 1 for i in image_page_indices]})"
            )
            return self._process_pdf_text_plus_images(
                llm_client, file_name, prompt, pdf_text,
                pdf_bytes, total_pages, all_page_indices,
                scanned_page_indices=image_page_indices,
            )

        # Fallback: render unavailable → text-only path
        logger.info(f"PDF render unavailable for {file_name}; using text-only path")
        truncated = False
        if len(pdf_text) > MAX_CHARS_PDF_TEXT:
            pdf_text = pdf_text[:MAX_CHARS_PDF_TEXT]
            truncated = True

        user_content = f"{prompt}\n\n--- CONTENIDO DEL PDF ---\n{pdf_text}"
        if truncated:
            user_content += "\n\n[...TEXTO TRUNCADO POR LÍMITE DE TAMAÑO...]"

        response = llm_client.chat(
            messages=[
                {"role": "system", "content": PDF_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=1,
            response_format={"type": "json_object"},
            caller_function="AnalizarBoletasPompeyoFn.process_pdf",
        )

        if not response.get("success", True):
            return f"Error LLM al procesar PDF '{file_name}': {response.get('error')}"

        return response.get("content", "")

    def _analyze_pdf_pages(self, file_record: dict):
        """Download the PDF once and detect scanned/image-only pages.

        Returns (total_pages, image_page_indices, pdf_bytes). On any failure
        (no URL, download error, render lib missing) returns (0, [], None) so
        the caller safely falls back to the text-only path.
        """
        file_url = file_record.get("file_url", "")
        if not file_url:
            return 0, [], None
        try:
            import pypdfium2 as pdfium

            pdf_bytes = self._get_source_bytes(file_record)

            with _PDFIUM_LOCK:
                doc = pdfium.PdfDocument(pdf_bytes)
                total_pages = len(doc)
                image_pages = []
                for i in range(total_pages):
                    textpage = doc[i].get_textpage()
                    page_text = (textpage.get_text_range() or "").strip()
                    if len(page_text) < MIN_PAGE_TEXT_CHARS:
                        image_pages.append(i)
                doc.close()
            return total_pages, image_pages, pdf_bytes
        except Exception as e:
            logger.warning(f"PDF page analysis failed ({e}); using text-only path")
            return 0, [], None

    def _process_pdf_text_plus_images(
        self, llm_client: Any, file_name: str, prompt: str, pdf_text: str,
        pdf_bytes: bytes, total_pages: int, image_page_indices: list,
        scanned_page_indices: Optional[list] = None,
    ) -> str:
        """Send extracted text PLUS rendered pages to vision.

        image_page_indices: pages to render (all pages in normal flow).
        scanned_page_indices: pages detected as scanned/image-only (prompt note only).
        """
        import pypdfium2 as pdfium

        if len(pdf_text) > MAX_CHARS_PDF_TEXT:
            pdf_text = pdf_text[:MAX_CHARS_PDF_TEXT]

        pages_to_render = image_page_indices[:MAX_VISION_PAGES]
        rendered_pages = []
        with _PDFIUM_LOCK:
            doc = pdfium.PdfDocument(pdf_bytes)
            for page_num in pages_to_render:
                bitmap = doc[page_num].render(scale=VISION_DPI / 72)
                rendered_pages.append((page_num, bitmap.to_pil()))
            doc.close()

        image_parts = []
        for page_num, pil_image in rendered_pages:
            pil_image = self._orient_upright(
                pil_image, f"{file_name}:page:{page_num + 1}"
            )
            image_parts.append(self._pil_image_part(pil_image))

        rendered_human = [i + 1 for i in pages_to_render]
        if scanned_page_indices is not None:
            truncated_note = (
                f" (PDF tiene {total_pages} páginas; se muestran las primeras {len(pages_to_render)})"
                if total_pages > MAX_VISION_PAGES else ""
            )
            if scanned_page_indices:
                scanned_human = [i + 1 for i in scanned_page_indices]
                vision_note = (
                    f"Se adjuntan TODAS las páginas del PDF como imágenes (páginas {rendered_human})"
                    f"{truncated_note}. De estas, {len(scanned_page_indices)} son páginas "
                    f"escaneadas/imagen sin texto extraíble (páginas {scanned_human}). Combina el "
                    f"texto extraído y las imágenes para extraer TODOS los campos; datos clave "
                    f"(montos, comisión, cuotas, tablas) pueden estar en imágenes embebidas."
                )
            else:
                vision_note = (
                    f"Se adjuntan TODAS las páginas del PDF como imágenes (páginas {rendered_human})"
                    f"{truncated_note}. Combina el texto extraído y las imágenes para extraer TODOS "
                    f"los campos; datos clave (tablas, montos, comisiones) pueden estar en imágenes "
                    f"embebidas aunque el PDF tenga capa de texto."
                )
        else:
            vision_note = (
                f"Este PDF tiene {total_pages} páginas y {len(image_page_indices)} "
                f"son páginas ESCANEADAS/IMAGEN sin texto extraíble. Esas páginas se adjuntan como "
                f"imágenes a continuación (páginas {rendered_human}). Datos clave (montos, comisión, "
                f"cuotas, saldos, tablas resumen) pueden estar SOLO en esas imágenes. Combina el texto "
                f"y las imágenes para extraer TODOS los campos; no marques un campo como ausente sin "
                f"haber revisado también las imágenes."
            )
        text_content = (
            f"{prompt}\n\n--- CONTENIDO DE TEXTO DEL PDF ({file_name}) ---\n{pdf_text}\n\n"
            f"NOTA IMPORTANTE: {vision_note}"
        )
        content_parts = [{"type": "text", "text": text_content}] + image_parts

        response = self._chat_with_ensemble(
            llm_client,
            messages=[
                {"role": "system", "content": PDF_SYSTEM_PROMPT},
                {"role": "user", "content": content_parts},
            ],
            caller_function="AnalizarBoletasPompeyoFn.process_pdf_mixed",
            extraction_context=prompt,
            temperature=1,
            response_format={"type": "json_object"},
        )

        if not response.get("success", True):
            return f"Error LLM al procesar PDF mixto '{file_name}': {response.get('error')}"

        return response.get("content", "")

    def _process_pdf_as_image(self, llm_client: Any, file_record: dict, prompt: str) -> str:
        """Vision fallback for PDFs with no extractable text (scanned docs).

        Downloads the PDF, renders each page as PNG using pypdfium2,
        and sends the page images to the vision model for analysis.
        """
        import requests
        import pypdfium2 as pdfium

        file_uuid = file_record["file_uuid"]
        file_name = file_record.get("file_name", file_uuid)
        file_url = file_record.get("file_url", "")

        if not file_url:
            return f"El PDF '{file_name}' no contiene texto extraíble y no tiene URL para análisis visual."

        # Download PDF bytes
        pdf_response = requests.get(file_url, timeout=30)
        if pdf_response.status_code != 200:
            return f"Error descargando PDF '{file_name}': HTTP {pdf_response.status_code}"

        # Render pages to PNG images using pypdfium2 (serialized — not thread-safe)
        rendered_pages = []
        with _PDFIUM_LOCK:
            doc = pdfium.PdfDocument(pdf_response.content)
            total_pages = len(doc)
            pages_to_render = min(total_pages, MAX_VISION_PAGES)

            logger.info(f"Rendering {pages_to_render}/{total_pages} pages of {file_name} as images")

            for page_num in range(pages_to_render):
                page = doc[page_num]
                bitmap = page.render(scale=VISION_DPI / 72)  # pypdfium2 uses scale factor (72 DPI base)
                rendered_pages.append((page_num, bitmap.to_pil()))
            doc.close()

        image_parts = []
        for page_num, pil_image in rendered_pages:
            pil_image = self._orient_upright(
                pil_image, f"{file_name}:page:{page_num + 1}"
            )
            image_parts.append(self._pil_image_part(pil_image))

        # Build message content: text prompt + all page images
        text_content = (
            f"{prompt}\n\nEste PDF ({file_name}) no contiene texto extraíble "
            f"(documento escaneado, {total_pages} páginas)."
        )
        if pages_to_render < total_pages:
            text_content += f" Se analizan las primeras {pages_to_render} páginas."
        text_content += "\nAnaliza visualmente el contenido de las páginas adjuntas."

        content_parts = [{"type": "text", "text": text_content}] + image_parts

        response = self._chat_with_ensemble(
            llm_client,
            messages=[
                {"role": "system", "content": PDF_SYSTEM_PROMPT},
                {"role": "user", "content": content_parts},
            ],
            caller_function="AnalizarBoletasPompeyoFn.process_pdf_vision",
            extraction_context=prompt,
            temperature=1,
            response_format={"type": "json_object"},
        )

        if not response.get("success", True):
            return f"Error LLM al procesar PDF visualmente '{file_name}': {response.get('error')}"

        return response.get("content", "")

    # ─────────────────────────────────────────────────────────────────────────
    # Image processing
    # ─────────────────────────────────────────────────────────────────────────

    def _process_single_image(self, llm_client: Any, file_record: dict, prompt: str) -> str:
        from PIL import Image

        file_uuid = file_record["file_uuid"]
        file_name = file_record.get("file_name", file_uuid)
        image_url = file_record.get("file_url", "")

        logger.info(f"Processing image: {file_name} ({file_uuid})")

        if not image_url:
            return f"Error: No se encontró URL para la imagen '{file_name}'"

        try:
            image_bytes = self._get_source_bytes(file_record)
            pil_image = Image.open(io.BytesIO(image_bytes))
            pil_image = self._orient_upright(pil_image, file_name)
            image_part = self._pil_image_part(pil_image)
        except Exception as e:
            logger.warning(
                f"Image orientation preflight failed for {file_name} ({e}); "
                "falling back to original URL"
            )
            image_part = {
                "type": "image_url",
                "image_url": {"url": image_url, "detail": "high"},
            }

        response = self._chat_with_ensemble(
            llm_client,
            messages=[
                {"role": "system", "content": IMAGE_SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    image_part,
                ]},
            ],
            caller_function="AnalizarBoletasPompeyoFn.process_image",
            extraction_context=prompt,
            temperature=1,
            response_format={"type": "json_object"},
        )

        if not response.get("success", True):
            return f"Error LLM al procesar imagen '{file_name}': {response.get('error')}"

        return response.get("content", "")

    # ─────────────────────────────────────────────────────────────────────────
    # DOCX processing
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_docx_text(self, zf) -> str:
        """Extract text from word/document.xml by iterating <w:p> and <w:t> tags."""
        import xml.etree.ElementTree as ET

        ns = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
        xml_content = zf.read('word/document.xml')
        root = ET.fromstring(xml_content)

        paragraphs = []
        for para in root.iter(f'{ns}p'):
            texts = []
            for t_elem in para.iter(f'{ns}t'):
                if t_elem.text:
                    texts.append(t_elem.text)
            if texts:
                paragraphs.append(''.join(texts))
        return '\n'.join(paragraphs)

    def _extract_docx_images(self, zf) -> List[dict]:
        """Extract raster images from word/media/ in DOCX ZIP."""
        mime_map = {
            '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
            '.gif': 'image/gif', '.bmp': 'image/bmp',
            '.tiff': 'image/tiff', '.tif': 'image/tiff',
        }
        images = []
        for entry in zf.namelist():
            if not entry.startswith('word/media/'):
                continue
            ext = os.path.splitext(entry)[1].lower()
            if ext not in RASTER_EXTS:
                continue
            img_bytes = zf.read(entry)
            mime = mime_map.get(ext, 'image/png')
            images.append({
                'name': os.path.basename(entry),
                'mime': mime,
                'bytes': img_bytes,
            })
            if len(images) >= MAX_DOCX_IMAGES:
                break
        return images

    def _process_single_docx(self, llm_client: Any, file_record: dict, prompt: str) -> str:
        """Process a DOCX file: extract text and embedded images, send to LLM."""
        import io
        import zipfile

        file_uuid = file_record["file_uuid"]
        file_name = file_record.get("file_name", file_uuid)
        file_url = file_record.get("file_url", "")

        logger.info(f"Processing DOCX: {file_name} ({file_uuid})")

        if not file_url:
            return f"Error: No se encontró URL para el documento '{file_name}'"

        try:
            docx_bytes = self._get_source_bytes(file_record)
            zf = zipfile.ZipFile(io.BytesIO(docx_bytes))
        except zipfile.BadZipFile:
            return f"Error: El archivo '{file_name}' no es un DOCX válido"

        # Extract text
        try:
            docx_text = self._extract_docx_text(zf).strip()
        except KeyError:
            docx_text = ""
            logger.warning(f"No word/document.xml found in {file_name}")

        # Extract embedded images
        embedded_images = self._extract_docx_images(zf)
        zf.close()

        logger.info(
            f"DOCX {file_name}: {len(docx_text)} chars text, "
            f"{len(embedded_images)} embedded images"
        )

        if not docx_text and not embedded_images:
            return f"El documento '{file_name}' no contiene texto ni imágenes extraíbles."

        # Truncate text if needed
        truncated = False
        if len(docx_text) > MAX_CHARS_PDF_TEXT:
            docx_text = docx_text[:MAX_CHARS_PDF_TEXT]
            truncated = True

        # Build LLM request
        if embedded_images:
            # Multipart: text + images
            text_content = f"{prompt}\n\n--- CONTENIDO DEL DOCUMENTO DOCX ---\n{docx_text}"
            if truncated:
                text_content += "\n\n[...TEXTO TRUNCADO POR LÍMITE DE TAMAÑO...]"
            if not docx_text:
                text_content = (
                    f"{prompt}\n\nEl documento '{file_name}' no contiene texto "
                    "extraíble. Analiza las imágenes embebidas."
                )
            text_content += f"\n\nEl documento contiene {len(embedded_images)} imagen(es) embebida(s)."

            content_parts = [{"type": "text", "text": text_content}]
            for img in embedded_images:
                content_parts.append(self._embedded_image_part(img))

            llm_response = self._chat_with_ensemble(
                llm_client,
                messages=[
                    {"role": "system", "content": DOCX_SYSTEM_PROMPT},
                    {"role": "user", "content": content_parts},
                ],
                caller_function="AnalizarBoletasPompeyoFn.process_docx_vision",
                extraction_context=prompt,
                temperature=1,
                response_format={"type": "json_object"},
            )
        else:
            # Text-only
            user_content = f"{prompt}\n\n--- CONTENIDO DEL DOCUMENTO DOCX ---\n{docx_text}"
            if truncated:
                user_content += "\n\n[...TEXTO TRUNCADO POR LÍMITE DE TAMAÑO...]"

            llm_response = llm_client.chat(
                messages=[
                    {"role": "system", "content": DOCX_SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                temperature=1,
                response_format={"type": "json_object"},
                caller_function="AnalizarBoletasPompeyoFn.process_docx",
            )

        if not llm_response.get("success", True):
            return f"Error LLM al procesar DOCX '{file_name}': {llm_response.get('error')}"

        return llm_response.get("content", "")

    # ─────────────────────────────────────────────────────────────────────────
    # Response building
    # ─────────────────────────────────────────────────────────────────────────

    def _build_processing_report(
        self,
        pdf_files: List[dict],
        image_files: List[dict],
        docx_files: List[dict],
        skipped: List[Tuple[str, str]],
    ) -> str:
        pdf_names = [f.get("file_name", "?") for f in pdf_files]
        img_names = [f.get("file_name", "?") for f in image_files]
        docx_names = [f.get("file_name", "?") for f in docx_files]

        ok_pdfs = ", ".join(pdf_names) if pdf_names else "—"
        ok_imgs = ", ".join(img_names) if img_names else "—"
        ok_docx = ", ".join(docx_names) if docx_names else "—"
        skip_report = (
            "\n".join(f"  - {name}: {reason}" for name, reason in skipped)
            if skipped else "—"
        )

        return (
            f"PDFs procesados ({len(pdf_files)}): {ok_pdfs}\n"
            f"DOCX procesados ({len(docx_files)}): {ok_docx}\n"
            f"Imagenes procesadas ({len(image_files)}): {ok_imgs}\n"
            f"Omitidos ({len(skipped)}):\n{skip_report}"
        )

    def _parse_extraction_content(self, content: str, file_name: str) -> Optional[dict]:
        if not isinstance(content, str):
            return None
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"Could not parse LLM JSON for {file_name}: {e}")
            parsed = self._recover_labeled_receipt_text(content)
            if parsed is None:
                return None
            logger.info(
                "deterministic_text_fallback_status=recovered file_name=%s",
                file_name,
            )
        if not isinstance(parsed, dict):
            logger.warning(f"LLM JSON for {file_name} is not an object")
            return None
        self._apply_rut_dv_check(parsed)
        normalize_financial_candidates(parsed)
        return parsed

    def _recover_labeled_receipt_text(self, content: str) -> Optional[dict]:
        """Recover conservative receipt facts from malformed JSON/plain text.

        This is intentionally narrow: it only uses explicitly labeled receipt
        fields and never treats unlabeled identity/date/folio numbers as money.
        """
        if not isinstance(content, str) or not content.strip():
            return None

        fields: Dict[str, dict] = {}
        amount_candidates = self._recover_amount_candidates_from_text(content)
        if amount_candidates:
            best_amount = amount_candidates[0]
            fields["monto_total"] = {
                "valor": best_amount["value"],
                "confianza": 72,
                "texto_contexto": best_amount.get("source", {}).get("context"),
            }
            if best_amount.get("currency"):
                fields["monto_total"]["moneda"] = best_amount["currency"]

        labeled_specs = (
            ("proveedor", ("proveedor", "proveedor_razon_social", "comercio", "razon_social")),
            ("fecha", ("fecha", "fecha_emision")),
            ("numero_folio", ("numero_folio", "folio", "boleta", "numero_boleta")),
            ("rut_proveedor", ("rut_proveedor", "rut")),
        )
        for output_name, labels in labeled_specs:
            value = self._recover_first_labeled_value(content, labels)
            if value:
                fields[output_name] = {"valor": value, "confianza": 70}

        category_name = self._recover_first_labeled_value(
            content,
            ("categoria", "category", "rubro"),
            nested_value_keys=("name", "nombre", "valor", "value"),
        )
        if category_name:
            fields["categoria_sugerida"] = {"valor": category_name, "confianza": 65}

        if not fields and not amount_candidates:
            return None

        receipt = {
            "receipt_discriminator": self._fallback_receipt_discriminator(fields),
            "page_metadata": self._recover_page_metadata(content),
            "tipo_documento": "comprobante",
            "description": "Extraccion recuperada desde texto no JSON con etiquetas explicitas.",
            "campos_extraidos": fields,
            "amount_candidates": amount_candidates,
            "extraction_confidence": 70 if amount_candidates else 45,
            "extraction_provenance": "deterministic_text_fallback",
            "ocr_text": content[:1200],
        }
        return {
            "receipts": [receipt],
            "extraction_confidence": receipt["extraction_confidence"],
            "extraction_provenance": "deterministic_text_fallback",
            "observaciones": (
                "Respuesta LLM no era JSON valido; se recuperaron solo campos "
                "explicitamente etiquetados."
            ),
        }

    def _recover_page_metadata(self, content: str) -> dict:
        page = self._recover_first_labeled_value(
            content,
            ("page_index", "page_number", "pagina"),
        )
        try:
            page_int = int(str(page))
        except (TypeError, ValueError):
            page_int = None
        if page_int is None:
            return {}
        return {"page_index": page_int, "page_range": [page_int, page_int]}

    def _fallback_receipt_discriminator(self, fields: Dict[str, dict]) -> str:
        parts = []
        for name in ("numero_folio", "proveedor", "fecha", "monto_total"):
            value = fields.get(name, {}).get("valor")
            if value:
                parts.append(f"{name}:{value}")
        return "|".join(parts) if parts else "deterministic_text_fallback"

    def _recover_first_labeled_value(
        self,
        content: str,
        labels: Tuple[str, ...],
        nested_value_keys: Tuple[str, ...] = ("valor", "value"),
    ) -> Optional[str]:
        for label in labels:
            object_value = self._recover_jsonish_object_value(
                content,
                label,
                nested_value_keys,
            )
            if object_value:
                return object_value
            scalar_value = self._recover_scalar_labeled_value(content, label)
            if scalar_value:
                return scalar_value
        return None

    def _recover_jsonish_object_value(
        self,
        content: str,
        label: str,
        value_keys: Tuple[str, ...],
    ) -> Optional[str]:
        label_pattern = re.escape(label)
        object_match = re.search(
            rf'"{label_pattern}"\s*:\s*\{{(?P<body>.{{0,1200}}?)\}}',
            content,
            re.IGNORECASE | re.DOTALL,
        )
        if not object_match:
            return None
        body = object_match.group("body")
        for key in value_keys:
            key_pattern = re.escape(key)
            value_match = re.search(
                rf'"{key_pattern}"\s*:\s*(?:"(?P<string>[^"]+)"|(?P<number>-?\d+(?:[.,]\d+)?))',
                body,
                re.IGNORECASE,
            )
            if value_match:
                value = value_match.group("string") or value_match.group("number")
                return value.strip() if value else None
        return None

    def _recover_scalar_labeled_value(self, content: str, label: str) -> Optional[str]:
        label_pattern = re.escape(label).replace(r"\_", r"[_\s-]?")
        match = re.search(
            rf"(?im)^\s*(?:{label_pattern})\s*[:#-]\s*(?P<value>[^\n\r]{{1,120}})",
            content,
        )
        if not match:
            return None
        value = match.group("value").strip().strip('"').strip()
        return value or None

    def _recover_amount_candidates_from_text(self, content: str) -> List[dict]:
        candidates = []
        seen = set()
        for candidate in self._recover_jsonish_amount_candidates(content):
            self._append_recovered_amount_candidate(candidates, seen, candidate)
        for candidate in self._recover_plain_total_amount_candidates(content):
            self._append_recovered_amount_candidate(candidates, seen, candidate)
        return candidates

    def _append_recovered_amount_candidate(
        self,
        candidates: List[dict],
        seen: set,
        candidate: dict,
    ) -> None:
        numeric_value = self._parse_numeric_value(candidate.get("value"))
        if numeric_value is None:
            numeric_value = self._parse_numeric_value(candidate.get("context"))
        if numeric_value is None:
            return
        dedupe_key = (candidate.get("label"), numeric_value, candidate.get("context"))
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        candidates.append({
            "label": candidate.get("label") or "monto_total",
            "value": candidate.get("value"),
            "numeric_value": numeric_value,
            "currency": candidate.get("currency"),
            "confidence": 0.72,
            "source": {
                "type": "deterministic_text_fallback",
                "context": candidate.get("context"),
            },
        })

    def _recover_jsonish_amount_candidates(self, content: str) -> List[dict]:
        recovered = []
        for label in ("monto_total", "total", "monto"):
            value = self._recover_jsonish_object_value(content, label, ("valor", "value", "numeric_value"))
            if not value:
                continue
            object_match = re.search(
                rf'"{re.escape(label)}"\s*:\s*\{{(?P<body>.{{0,1200}}?)\}}',
                content,
                re.IGNORECASE | re.DOTALL,
            )
            body = object_match.group("body") if object_match else ""
            currency = self._recover_jsonish_object_value(content, label, ("moneda", "currency"))
            context_match = re.search(
                r'"texto_contexto"\s*:\s*"(?P<context>[^"]+)"',
                body,
                re.IGNORECASE,
            )
            context = context_match.group("context") if context_match else None
            if label == "monto" and not (currency or self._detect_currency(context)):
                continue
            recovered.append({
                "label": "monto_total" if label in ("total", "monto_total") else "monto",
                "value": value,
                "currency": currency or self._detect_currency(context),
                "context": context or f"{label}: {value}",
            })
        return recovered

    def _recover_plain_total_amount_candidates(self, content: str) -> List[dict]:
        recovered = []
        pattern = re.compile(
            r"(?im)\b(?P<label>monto\s+total|total)\b"
            r"(?P<context>[^\n\r]{0,80}?(?:\$|CLP|USD|UF)[^\n\r]{0,40})",
        )
        for match in pattern.finditer(content):
            context = f"{match.group('label')} {match.group('context')}".strip()
            value_match = re.search(
                r"(?<![\w-])(\d{1,3}(?:[.,]\d{3})+(?:,\d{1,2})?|\d{4,})(?![\w-])",
                context,
            )
            if not value_match:
                continue
            recovered.append({
                "label": "monto_total",
                "value": value_match.group(1),
                "currency": self._detect_currency(context),
                "context": context,
            })
        return recovered

    def _get_source_bytes(self, file_record: dict) -> bytes:
        file_uuid = file_record.get("file_uuid") or file_record.get("uuid")
        if not file_uuid:
            raise ValueError("File record is missing file_uuid")
        if "content_bytes" in file_record:
            content = file_record["content_bytes"]
            if isinstance(content, str):
                content = content.encode("utf-8")
            self._source_bytes_cache[file_uuid] = bytes(content)
            return self._source_bytes_cache[file_uuid]
        cached = self._source_bytes_cache.get(file_uuid)
        if cached is not None:
            return cached
        file_url = file_record.get("file_url", "")
        if not file_url:
            raise ValueError(f"No source URL available for file {file_uuid}")
        import requests
        response = requests.get(file_url, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Could not download source bytes for file {file_uuid}: HTTP {response.status_code}"
            )
        self._source_bytes_cache[file_uuid] = response.content
        return response.content

    def _metadata_hash(self, file_record: dict) -> str:
        """Stable metadata hash. It avoids exposing URLs and is not idempotency input."""
        identity = {
            "file_uuid": file_record.get("file_uuid"),
            "file_name": file_record.get("file_name"),
            "mime_type": file_record.get("mime_type") or file_record.get("content_type"),
            "size": file_record.get("size") or file_record.get("file_size"),
        }
        payload = json.dumps(identity, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _source_content_sha256(self, file_record: dict) -> Optional[str]:
        try:
            return hashlib.sha256(self._get_source_bytes(file_record)).hexdigest()
        except Exception as exc:
            logger.warning(
                "source_content_hash_status=unavailable file_uuid=%s error=%s",
                file_record.get("file_uuid"),
                exc,
            )
            return None

    def _file_kind(self, file_record: dict) -> str:
        if self._is_pdf(file_record):
            return "pdf"
        if self._is_docx(file_record):
            return "docx"
        if self._is_image(file_record):
            return "image"
        if self._is_video(file_record):
            return "video"
        return "unsupported"

    def _inventory_entry(
        self,
        file_record: dict,
        status: str = "processed",
        reason: Optional[str] = None,
    ) -> dict:
        entry = {
            "file_uuid": file_record.get("file_uuid"),
            "file_name": file_record.get("file_name"),
            "mime_type": file_record.get("mime_type") or file_record.get("content_type"),
            "kind": self._file_kind(file_record),
            "status": status,
            "source_content_sha256": self._source_content_sha256(file_record),
            "source_content_hash_algorithm": "sha256(source_bytes)",
            "source_metadata_sha256": self._metadata_hash(file_record),
            "source_metadata_hash_algorithm": "sha256(file_uuid|file_name|mime_type|size)",
            "page_metadata": {
                "known_total_pages": file_record.get("page_count"),
                "processed_pages": file_record.get("processed_pages"),
                "page_index": file_record.get("page_index"),
                "page_range": file_record.get("page_range"),
            },
        }
        if reason:
            entry["reason"] = reason
        return entry

    def _confidence_0_1(self, raw: Any) -> float:
        """Normalize confidence to 0..1. Values >1 are interpreted as 0..100."""
        if raw is None:
            return 0.0
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 0.0
        if value > 1:
            value = value / 100.0
        return round(max(0.0, min(1.0, value)), 4)

    def _looks_like_secret(self, value: Any) -> bool:
        if not isinstance(value, str):
            return False
        text = value.strip()
        if len(text) < 16:
            return False
        return bool(re.search(
            r"(sk-[A-Za-z0-9_-]{12,}|xox[baprs]-|gh[pousr]_[A-Za-z0-9_]{12,}|"
            r"AKIA[0-9A-Z]{12,}|eyJ[A-Za-z0-9_-]{20,})",
            text,
        ))

    def _redact_secret_values(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._redact_secret_values(child) for key, child in value.items()}
        if isinstance(value, list):
            return [self._redact_secret_values(child) for child in value]
        if self._looks_like_secret(value):
            return "[REDACTED_SECRET]"
        return value

    def _field_source(self, field_name: str, field_data: Any, file_record: dict) -> dict:
        source = {
            "file_uuid": file_record.get("file_uuid"),
            "file_name": file_record.get("file_name"),
            "source_content_sha256": self._source_content_sha256(file_record),
            "source_metadata_sha256": self._metadata_hash(file_record),
            "field": field_name,
        }
        if isinstance(field_data, dict):
            page = (
                field_data.get("pagina")
                or field_data.get("page")
                or field_data.get("page_number")
                or field_data.get("numero_pagina")
            )
            if page is not None:
                source["page"] = page
                source["page_metadata"] = {"page_index": page, "page_range": [page, page]}
            if field_data.get("texto_contexto"):
                source["context"] = field_data.get("texto_contexto")
        if "page_metadata" not in source:
            source["page_metadata"] = {
                "page_index": file_record.get("page_index"),
                "page_range": file_record.get("page_range"),
            }
        return source

    def _canonical_receipt_date_value(self, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        text = value.strip()
        if not text:
            return value

        iso = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
        if iso:
            year, month, day = (int(part) for part in iso.groups())
            try:
                return datetime(year, month, day).date().isoformat()
            except ValueError:
                return value

        chilean = re.fullmatch(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", text)
        if not chilean:
            return value
        day, month, year = (int(part) for part in chilean.groups())
        if day <= 12 and month <= 12:
            return value
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            return value

    def _canonical_receipt_contract_value(self, field_name: str, value: Any) -> Any:
        normalized_name = self._strip_accents_casefold(str(field_name))
        if normalized_name in {"date", "fecha", "fecha_emision"}:
            return self._canonical_receipt_date_value(value)
        return value

    def _iter_extracted_fields(self, parsed: Optional[dict], file_record: dict):
        if not parsed:
            return
        fields = parsed.get("campos_extraidos")
        existing_fields = set()
        if isinstance(fields, dict):
            for field_name, field_data in fields.items():
                existing_fields.add(self._strip_accents_casefold(str(field_name)))
                if isinstance(field_data, dict):
                    value = field_data.get("valor")
                    confidence = field_data.get("confianza", field_data.get("confidence"))
                else:
                    value = field_data
                    confidence = None
                value = self._canonical_receipt_contract_value(field_name, value)
                yield field_name, field_data, {
                    "field": field_name,
                    "value": self._redact_secret_values(value),
                    "confidence": self._confidence_0_1(confidence),
                    "source": self._field_source(field_name, field_data, file_record),
                }
        yield from self._iter_direct_audit_fields(parsed, file_record, existing_fields)

    def _iter_direct_audit_fields(self, parsed: Optional[dict], file_record: dict, existing_fields: set):
        if not isinstance(parsed, dict):
            return
        for field_name, aliases in (
            ("provider", PROVIDER_ALIASES),
            ("folio", ("folio", "numero_folio", "receipt_number", "numero_o_folio", "numero_boleta", "numero_documento")),
            ("date", ("date", "fecha", "fecha_emision")),
            ("rut", ("rut", "rut_proveedor", "rut_opcional", "tax_id")),
            ("currency", ("currency", "moneda")),
            ("detail", ("detail", "detalle", "items_visibles")),
        ):
            if field_name in existing_fields:
                continue
            value = self._field_value_for_aliases(parsed, aliases)
            if value in (None, ""):
                continue
            value = self._canonical_receipt_contract_value(field_name, value)
            confidence = parsed.get("confidence") or parsed.get("extraction_confidence")
            field_data = {"valor": value, "confianza": confidence}
            yield field_name, field_data, {
                "field": field_name,
                "value": self._redact_secret_values(value),
                "confidence": self._confidence_0_1(confidence),
                "source": self._field_source(field_name, field_data, file_record),
            }

    def _is_amount_field(self, field_name: str, value: Any) -> bool:
        label = self._strip_accents_casefold(field_name)
        if any(token in label for token in (
            "monto", "total", "iva", "neto", "subtotal", "propina", "descuento",
            "valor", "precio", "saldo", "cargo", "pago",
        )):
            return True
        if isinstance(value, str) and re.search(r"(\$|clp|usd|uf)\s*[\d.,]+|[\d.,]+\s*(clp|usd|uf)", value, re.I):
            return True
        return False

    def _amount_value_from_mapping(self, data: dict) -> Any:
        for key in ("valor", "value", "numeric_value", "monto", "amount"):
            if key in data and data.get(key) not in (None, ""):
                return data.get(key)
        return None

    def _amount_confidence_from_mapping(self, data: dict) -> Any:
        for key in ("confianza", "confidence", "confidence_score"):
            if key in data:
                return data.get(key)
        return None

    def _amount_label_from_mapping(self, fallback: str, data: dict) -> str:
        label = (
            data.get("campo_normalizado")
            or data.get("etiqueta")
            or data.get("label")
            or data.get("field")
            or data.get("source")
            or fallback
        )
        return str(label)

    def _append_amount_candidate(
        self,
        candidates: List[dict],
        seen: set,
        file_record: dict,
        field_name: str,
        field_data: Any,
        value: Any,
        confidence: Any,
        currency: Optional[str] = None,
        id_suffix: Optional[str] = None,
    ) -> None:
        if value in (None, ""):
            return
        if not self._is_amount_field(field_name, value):
            return
        normalized_value = self._redact_secret_values(value)
        numeric_value = self._parse_numeric_value(normalized_value)
        if numeric_value is None and isinstance(field_data, dict):
            context = field_data.get("texto_contexto") or field_data.get("context")
            numeric_value = self._parse_numeric_value(context)
        candidate_id = f"{file_record.get('file_uuid')}:{id_suffix or field_name}"
        dedupe_key = (
            candidate_id,
            str(normalized_value),
            numeric_value,
            self._strip_accents_casefold(field_name),
        )
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        detected_currency = (
            currency
            or (field_data.get("moneda") if isinstance(field_data, dict) else None)
            or (field_data.get("currency") if isinstance(field_data, dict) else None)
            or self._detect_currency(normalized_value)
            or self._detect_currency(field_data.get("texto_contexto") if isinstance(field_data, dict) else None)
        )
        candidates.append({
            "id": candidate_id,
            "label": field_name,
            "value": normalized_value,
            "numeric_value": numeric_value,
            "currency": detected_currency,
            "confidence": self._confidence_0_1(confidence),
            "source": self._field_source(field_name, field_data, file_record),
        })

    def _explicit_amount_candidates(self, parsed: Optional[dict], file_record: dict):
        if not isinstance(parsed, dict):
            return
        containers = (
            "monto", "amount", "monto_total", "total", "proposed_amount",
            "importe", "valor_total",
        )
        for key in containers:
            data = parsed.get(key)
            if isinstance(data, dict):
                label = self._amount_label_from_mapping(key, data)
                yield key, label, data, self._amount_value_from_mapping(data), data
                nested = data.get("candidatos") or data.get("candidates") or data.get("amount_candidates")
                if isinstance(nested, list):
                    for index, item in enumerate(nested):
                        if not isinstance(item, dict):
                            continue
                        nested_label = self._amount_label_from_mapping(label, item)
                        yield f"{key}.candidatos.{index}", nested_label, item, self._amount_value_from_mapping(item), item
            elif data not in (None, ""):
                yield key, key, {"valor": data}, data, {"valor": data}

        for list_key in ("amount_candidates", "candidatos_monto", "monto_candidates"):
            nested = parsed.get(list_key)
            if not isinstance(nested, list):
                continue
            for index, item in enumerate(nested):
                if not isinstance(item, dict):
                    continue
                label = self._amount_label_from_mapping(list_key, item)
                yield f"{list_key}.{index}", label, item, self._amount_value_from_mapping(item), item

    def _amount_candidates(self, parsed: Optional[dict], file_record: dict) -> List[dict]:
        candidates = []
        seen = set()
        for field_name, field_data, normalized in self._iter_extracted_fields(parsed, file_record) or []:
            self._append_amount_candidate(
                candidates,
                seen,
                file_record,
                field_name,
                field_data,
                normalized["value"],
                normalized["confidence"],
            )
        for id_suffix, field_name, field_data, value, confidence_source in self._explicit_amount_candidates(parsed, file_record) or []:
            self._append_amount_candidate(
                candidates,
                seen,
                file_record,
                field_name,
                field_data,
                value,
                self._amount_confidence_from_mapping(confidence_source),
                currency=field_data.get("moneda") if isinstance(field_data, dict) else None,
                id_suffix=id_suffix,
            )
        return candidates

    def _amount_priority(self, candidate: dict) -> Tuple[int, float]:
        label = self._strip_accents_casefold(str(candidate.get("label", "")))
        priority = 0
        if "total" in label or "monto_total" in label or "monto total" in label:
            priority = 4
        elif "iva" in label or "neto" in label or "subtotal" in label:
            priority = 2
        elif any(token in label for token in ("monto", "valor", "pago")):
            priority = 3
        confidence = float(candidate.get("confidence") or 0)
        return priority, confidence

    def _proposed_amount(self, amount_candidates: List[dict]) -> dict:
        valid = [
            candidate for candidate in amount_candidates
            if candidate.get("numeric_value") is not None
        ]
        if not valid:
            return {
                "value": None,
                "numeric_value": None,
                "currency": None,
                "confidence": 0.0,
                "source": None,
                "candidate_id": None,
                "ambiguous": True,
                "selection_rule": "no_numeric_amount_candidate",
            }
        ordered = sorted(valid, key=self._amount_priority, reverse=True)
        top = ordered[0]
        runner_up = next(
            (
                candidate for candidate in ordered[1:]
                if candidate.get("numeric_value") != top.get("numeric_value")
            ),
            None,
        )
        ambiguous = False
        if runner_up is not None:
            same_priority = self._amount_priority(top)[0] == self._amount_priority(runner_up)[0]
            close_confidence = abs((top.get("confidence") or 0) - (runner_up.get("confidence") or 0)) < 0.15
            different_value = top.get("numeric_value") != runner_up.get("numeric_value")
            ambiguous = same_priority and close_confidence and different_value
        return {
            "value": top.get("value"),
            "numeric_value": top.get("numeric_value"),
            "currency": top.get("currency"),
            "confidence": top.get("confidence", 0.0),
            "source": top.get("source"),
            "candidate_id": top.get("id"),
            "ambiguous": ambiguous,
            "selection_rule": "prefer_total_label_then_confidence",
        }

    def _normalize_page_range(self, page_metadata: Any) -> Tuple[Optional[int], Optional[List[int]], Optional[str]]:
        if not isinstance(page_metadata, dict):
            return None, None, None
        page_index = (
            page_metadata.get("page_index")
            or page_metadata.get("page")
            or page_metadata.get("pagina")
            or page_metadata.get("page_number")
        )
        page_range = page_metadata.get("page_range") or page_metadata.get("pages")
        normalized_index = None
        try:
            if page_index is not None:
                normalized_index = int(page_index)
        except (TypeError, ValueError):
            normalized_index = None
        normalized_range = None
        if isinstance(page_range, list) and page_range:
            numeric_pages = []
            for item in page_range[:2]:
                try:
                    numeric_pages.append(int(item))
                except (TypeError, ValueError):
                    pass
            if len(numeric_pages) == 1:
                normalized_range = [numeric_pages[0], numeric_pages[0]]
            elif len(numeric_pages) >= 2:
                start, end = numeric_pages[0], numeric_pages[1]
                normalized_range = [min(start, end), max(start, end)]
        elif isinstance(page_range, str):
            found = [int(num) for num in re.findall(r"\d+", page_range)]
            if len(found) == 1:
                normalized_range = [found[0], found[0]]
            elif len(found) >= 2:
                normalized_range = [min(found[0], found[1]), max(found[0], found[1])]
        if normalized_range is None and normalized_index is not None:
            normalized_range = [normalized_index, normalized_index]
        group_label = (
            page_metadata.get("group_label")
            or page_metadata.get("receipt_group")
            or page_metadata.get("grupo")
        )
        return normalized_index, normalized_range, str(group_label) if group_label else None

    def _receipt_page_metadata(self, receipt_item: dict, file_record: dict) -> dict:
        page_index, page_range, group_label = self._normalize_page_range(
            receipt_item.get("page_metadata") or {}
        )
        if page_range is None:
            field_pages = []
            fields = receipt_item.get("campos_extraidos")
            if isinstance(fields, dict):
                for field_data in fields.values():
                    if not isinstance(field_data, dict):
                        continue
                    page = (
                        field_data.get("pagina")
                        or field_data.get("page")
                        or field_data.get("page_number")
                    )
                    try:
                        field_pages.append(int(page))
                    except (TypeError, ValueError):
                        pass
            if field_pages:
                page_range = [min(field_pages), max(field_pages)]
                page_index = min(field_pages)
        if page_range is None:
            fallback_range = file_record.get("page_range")
            if isinstance(fallback_range, list) and fallback_range:
                page_range = fallback_range
        return {
            "page_index": page_index,
            "page_range": page_range,
            "group_label": group_label,
        }

    def _receipt_discriminator(self, receipt_item: dict) -> str:
        direct = (
            receipt_item.get("receipt_discriminator")
            or receipt_item.get("discriminator")
            or receipt_item.get("group_label")
        )
        if direct:
            return str(direct)
        fields = receipt_item.get("campos_extraidos")
        parts = []
        if isinstance(fields, dict):
            for name in (
                "folio", "numero_boleta", "numero_documento", "proveedor",
                "proveedor_razon_social", "razon_social_o_proveedor", "razon_social",
                "rut_proveedor", "fecha", "fecha_emision", "monto_total",
                "total", "comercio",
            ):
                field = fields.get(name)
                if isinstance(field, dict):
                    value = field.get("valor")
                else:
                    value = field
                if value not in (None, ""):
                    parts.append(f"{name}:{value}")
        if not parts and receipt_item.get("description"):
            parts.append(str(receipt_item.get("description")))
        return "|".join(parts) if parts else "receipt"

    def _normalize_receipt_discriminator(self, value: str) -> str:
        normalized = self._strip_accents_casefold(value or "receipt")
        normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
        return normalized or "receipt"

    def _scalar_from_extracted_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            for key in ("valor", "value", "numeric_value", "name", "nombre"):
                if value.get(key) not in (None, ""):
                    return value.get(key)
            return None
        return value

    def _field_value_for_aliases(self, receipt_item: dict, aliases: Tuple[str, ...]) -> Any:
        if not isinstance(receipt_item, dict):
            return None
        fields = receipt_item.get("campos_extraidos")
        if isinstance(fields, dict):
            normalized_fields = {
                self._strip_accents_casefold(str(key)): value
                for key, value in fields.items()
            }
            for alias in aliases:
                value = self._scalar_from_extracted_value(
                    normalized_fields.get(self._strip_accents_casefold(alias))
                )
                if value not in (None, ""):
                    return value
        normalized_item = {
            self._strip_accents_casefold(str(key)): value
            for key, value in receipt_item.items()
        }
        for alias in aliases:
            value = self._scalar_from_extracted_value(
                normalized_item.get(self._strip_accents_casefold(alias))
            )
            if value not in (None, ""):
                return value
        return None

    def _receipt_audit_fields(self, receipt_item: dict) -> dict:
        audit = {}
        for canonical_name, aliases in (
            ("provider", PROVIDER_ALIASES),
            ("folio", ("folio", "numero_folio", "receipt_number", "numero_o_folio", "numero_boleta", "numero_documento")),
            ("date", ("date", "fecha", "fecha_emision")),
            ("rut", ("rut", "rut_proveedor", "rut_opcional", "tax_id")),
            ("currency", ("currency", "moneda")),
            ("detail", ("detail", "detalle", "items_visibles", "description")),
        ):
            value = self._field_value_for_aliases(receipt_item, aliases)
            if value not in (None, ""):
                value = self._canonical_receipt_contract_value(canonical_name, value)
                audit[canonical_name] = self._redact_secret_values(value)
        if "currency" not in audit:
            for amount_key in ("monto", "amount", "monto_total", "total", "proposed_amount"):
                amount_data = receipt_item.get(amount_key)
                if isinstance(amount_data, dict):
                    currency = amount_data.get("moneda") or amount_data.get("currency")
                    if currency not in (None, ""):
                        audit["currency"] = self._redact_secret_values(currency)
                        break
        return audit

    def _semantic_receipt_identity(self, file_record: dict, receipt_item: dict) -> dict:
        page_metadata = self._receipt_page_metadata(receipt_item, file_record)
        audit = self._receipt_audit_fields(receipt_item)
        amount = self._proposed_amount(self._amount_candidates(receipt_item, file_record))
        category = self._category_candidates(receipt_item, file_record)
        folio_value = audit.get("folio")
        has_folio = folio_value not in (None, "") and str(folio_value).strip() != ""
        identity = {
            "source_content_sha256": self._source_content_sha256(file_record),
            "page_index": page_metadata.get("page_index"),
            "page_range": page_metadata.get("page_range"),
            "group_label": self._normalize_receipt_discriminator(page_metadata.get("group_label") or ""),
            "provider": self._normalize_receipt_discriminator(str(audit.get("provider") or "")),
            "folio": self._normalize_receipt_discriminator(str(folio_value or "")),
            "date": self._normalize_receipt_discriminator(str(audit.get("date") or "")),
            "amount": amount.get("numeric_value"),
            "currency": self._normalize_receipt_discriminator(str(audit.get("currency") or amount.get("currency") or "")),
            "category_id": self._normalize_receipt_discriminator(str(category.get("id") or "")),
        }
        if not has_folio:
            identity["receipt_discriminator"] = self._normalize_receipt_discriminator(
                self._receipt_discriminator(receipt_item)
            )
        return identity

    def _stable_receipt_id(self, file_record: dict, receipt_item: dict) -> str:
        identity = self._semantic_receipt_identity(file_record, receipt_item)
        digest = hashlib.sha256(
            json.dumps(identity, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        return f"receipt_{digest[:24]}"

    def _receipt_quality_score(self, receipt: dict) -> Tuple[int, float]:
        amount = receipt.get("proposed_amount") or {}
        category = receipt.get("expense_category") or {}
        audit = receipt.get("audit_fields") or {}
        score = 0
        if receipt.get("parse_status") == "parsed":
            score += 10
        if amount.get("numeric_value") is not None:
            score += 8
        if category.get("id") not in (None, ""):
            score += 6
        for key in ("provider", "folio", "date", "rut", "currency"):
            if audit.get(key) not in (None, ""):
                score += 1
        confidence = receipt.get("extraction_confidence") or 0.0
        try:
            confidence_value = float(confidence)
        except (TypeError, ValueError):
            confidence_value = 0.0
        return score, confidence_value

    def _physical_receipt_dedupe_key(self, receipt: dict) -> Optional[str]:
        source = receipt.get("source") or {}
        audit = receipt.get("audit_fields") or {}
        source_digest = source.get("source_content_sha256")
        if not source_digest:
            return None
        page_metadata = source.get("page_metadata") or {}
        folio_value = audit.get("folio")
        if folio_value in (None, "") or str(folio_value).strip() == "":
            return None
        identity = {
            "source_content_sha256": source_digest,
            "page_range": page_metadata.get("page_range"),
            "folio": self._normalize_receipt_discriminator(str(folio_value)),
        }
        return json.dumps(identity, sort_keys=True, ensure_ascii=False)

    def _dedupe_receipts(self, receipts: List[dict]) -> List[dict]:
        deduped = []
        seen = set()
        physical_seen = {}
        for receipt in receipts:
            if receipt.get("parse_status") != "parsed":
                deduped.append(receipt)
                continue
            source = receipt.get("source") or {}
            audit = receipt.get("audit_fields") or {}
            amount = receipt.get("proposed_amount") or {}
            category = receipt.get("expense_category") or {}
            folio_value = audit.get("folio")
            has_folio = folio_value not in (None, "") and str(folio_value).strip() != ""
            identity = {
                "source_content_sha256": source.get("source_content_sha256"),
                "page_index": (source.get("page_metadata") or {}).get("page_index"),
                "page_range": (source.get("page_metadata") or {}).get("page_range"),
                "group_label": self._normalize_receipt_discriminator(
                    (source.get("page_metadata") or {}).get("group_label") or ""
                ),
                "provider": self._normalize_receipt_discriminator(str(audit.get("provider") or "")),
                "folio": self._normalize_receipt_discriminator(str(folio_value or "")),
                "date": self._normalize_receipt_discriminator(str(audit.get("date") or "")),
                "amount": amount.get("numeric_value"),
                "currency": self._normalize_receipt_discriminator(str(audit.get("currency") or amount.get("currency") or "")),
                "category_id": self._normalize_receipt_discriminator(str(category.get("id") or "")),
            }
            if not has_folio:
                identity["receipt_discriminator"] = self._normalize_receipt_discriminator(
                    source.get("receipt_discriminator") or ""
                )
            key = json.dumps(identity, sort_keys=True, ensure_ascii=False)
            physical_key = self._physical_receipt_dedupe_key(receipt)
            if physical_key and physical_key in physical_seen:
                existing_index = physical_seen[physical_key]
                existing = deduped[existing_index]
                if self._receipt_quality_score(receipt) > self._receipt_quality_score(existing):
                    logger.info(
                        "receipt_dedupe_status=duplicate_replaced old_receipt_id=%s new_receipt_id=%s source_file_uuid=%s",
                        existing.get("receipt_id"),
                        receipt.get("receipt_id"),
                        source.get("file_uuid"),
                    )
                    deduped[existing_index] = receipt
                else:
                    logger.info(
                        "receipt_dedupe_status=physical_duplicate_skipped receipt_id=%s source_file_uuid=%s",
                        receipt.get("receipt_id"),
                        source.get("file_uuid"),
                    )
                continue
            if key in seen:
                logger.info(
                    "receipt_dedupe_status=duplicate_skipped receipt_id=%s source_file_uuid=%s",
                    receipt.get("receipt_id"),
                    source.get("file_uuid"),
                )
                continue
            seen.add(key)
            if physical_key:
                physical_seen[physical_key] = len(deduped)
            deduped.append(receipt)
        return deduped

    def _receipt_items_from_parsed(self, parsed: Optional[dict]) -> Tuple[List[dict], Optional[str]]:
        if parsed is None:
            return [], "malformed_or_non_json_extraction"
        receipts = parsed.get("receipts")
        if not isinstance(receipts, list):
            receipts = parsed.get("ready_receipts")
        if isinstance(receipts, list):
            normalized = [item for item in receipts if isinstance(item, dict)]
            if normalized:
                return normalized, None
            return [], "no_receipts_detected"
        if isinstance(parsed.get("campos_extraidos"), dict):
            return [parsed], None
        return [], "no_receipts_detected"

    def _detect_currency(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        text = value.upper()
        if "UF" in text:
            return "UF"
        if "USD" in text or "US$" in text:
            return "USD"
        if "$" in text or "CLP" in text:
            return "CLP"
        return None

    def _parse_category_catalog_snapshot(self, raw_snapshot: Any) -> dict:
        """Parse an injected, versioned ROMA category snapshot.

        Until a safe read-only discovery step provides this snapshot, category
        IDs are intentionally unresolved so downstream stages cannot treat
        guessed IDs as authoritative ROMA IDs.
        """
        if raw_snapshot in (None, "", {}):
            return {
                "status": "unresolved",
                "version": None,
                "source": "not_provided",
                "categories": [],
            }
        if isinstance(raw_snapshot, str):
            try:
                raw_snapshot = json.loads(raw_snapshot)
            except json.JSONDecodeError:
                return {
                    "status": "unresolved",
                    "version": None,
                    "source": "invalid_json",
                    "categories": [],
                }
        if not isinstance(raw_snapshot, dict):
            return {
                "status": "unresolved",
                "version": None,
                "source": "invalid_type",
                "categories": [],
            }
        categories = raw_snapshot.get("categories")
        version = raw_snapshot.get("version")
        source = raw_snapshot.get("source", "injected_snapshot")
        if not version or not isinstance(categories, list):
            return {
                "status": "unresolved",
                "version": version,
                "source": source,
                "categories": [],
            }
        normalized = []
        for category in categories:
            if not isinstance(category, dict):
                continue
            category_id = category.get("id")
            name = category.get("name")
            if category_id is None or not name:
                continue
            keywords = category.get("keywords") or category.get("aliases") or []
            if isinstance(keywords, str):
                keywords = [keywords]
            normalized.append({
                "id": str(category_id),
                "name": str(name),
                "keywords": [str(keyword) for keyword in keywords],
            })
        status = "resolved" if normalized else "unresolved"
        return {
            "status": status,
            "version": str(version),
            "source": str(source),
            "categories": normalized,
        }

    def _category_text(self, parsed: Optional[dict]) -> str:
        if not parsed:
            return ""
        parts = []
        for key in ("tipo_documento", "file_name", "description", "key_data_table", "observaciones", "ocr_text"):
            value = parsed.get(key)
            if isinstance(value, str):
                parts.append(value)
        fields = parsed.get("campos_extraidos")
        if isinstance(fields, dict):
            for field_name, field_data in fields.items():
                parts.append(str(field_name))
                if isinstance(field_data, dict):
                    parts.append(str(field_data.get("valor", "")))
                    parts.append(str(field_data.get("texto_contexto", "")))
                else:
                    parts.append(str(field_data))
        for category_key in ("categoria", "category", "expense_category"):
            category = parsed.get(category_key)
            if not isinstance(category, dict):
                continue
            for value_key in ("name", "nombre", "valor", "value"):
                value = category.get(value_key)
                if isinstance(value, str):
                    parts.append(value)
        return self._strip_accents_casefold(" ".join(parts))

    def _category_candidates(
        self,
        parsed: Optional[dict],
        file_record: dict,
        category_catalog_snapshot: Optional[dict] = None,
    ) -> dict:
        snapshot = category_catalog_snapshot or self._parse_category_catalog_snapshot(None)
        if snapshot.get("status") != "resolved":
            return {
                "id": None,
                "name": None,
                "status": "unresolved",
                "catalog": {
                    "status": snapshot.get("status", "unresolved"),
                    "version": snapshot.get("version"),
                    "source": snapshot.get("source", "not_provided"),
                },
                "source": {
                    "type": "catalog_unresolved",
                    "file_uuid": file_record.get("file_uuid"),
                    "source_content_sha256": self._source_content_sha256(file_record),
                },
                "confidence": 0.0,
                "candidates": [],
                "ambiguous": True,
            }
        text = self._category_text(parsed)
        candidates = []
        for category in snapshot.get("categories", []):
            hits = [kw for kw in category["keywords"] if self._strip_accents_casefold(kw) in text]
            score = min(0.95, 0.35 + (0.2 * len(hits))) if hits else 0.0
            if score <= 0:
                continue
            candidates.append({
                "id": category["id"],
                "name": category["name"],
                "source": {
                    "type": "injected_snapshot_keyword",
                    "catalog_version": snapshot.get("version"),
                    "catalog_source": snapshot.get("source"),
                    "matched_terms": hits,
                    "file_uuid": file_record.get("file_uuid"),
                    "source_content_sha256": self._source_content_sha256(file_record),
                },
                "confidence": round(score, 4),
            })
        candidates.sort(key=lambda item: item["confidence"], reverse=True)
        if not candidates:
            return {
                "id": None,
                "name": None,
                "status": "unmatched",
                "catalog": {
                    "status": snapshot.get("status"),
                    "version": snapshot.get("version"),
                    "source": snapshot.get("source"),
                },
                "source": {
                    "type": "no_snapshot_keyword_match",
                    "file_uuid": file_record.get("file_uuid"),
                    "source_content_sha256": self._source_content_sha256(file_record),
                },
                "confidence": 0.0,
                "candidates": [],
                "ambiguous": True,
            }
        top = candidates[0]
        runner_up = candidates[1] if len(candidates) > 1 else None
        ambiguous = runner_up is not None and (top["confidence"] - runner_up["confidence"]) < 0.2
        return {
            "id": top["id"],
            "name": top["name"],
            "status": "resolved",
            "catalog": {
                "status": snapshot.get("status"),
                "version": snapshot.get("version"),
                "source": snapshot.get("source"),
            },
            "source": top["source"],
            "confidence": top["confidence"],
            "candidates": candidates,
            "ambiguous": ambiguous,
        }

    def _receipt_entry(
        self,
        file_record: dict,
        parsed: Optional[dict],
        receipt_item: Optional[dict],
        raw_content: str,
        category_catalog_snapshot: Optional[dict] = None,
        parse_error: Optional[str] = None,
    ) -> dict:
        receipt_item = receipt_item or {}
        fields = [
            normalized
            for _, _, normalized in (self._iter_extracted_fields(receipt_item, file_record) or [])
        ]
        amount_candidates = self._amount_candidates(receipt_item, file_record)
        page_metadata = self._receipt_page_metadata(receipt_item, file_record)
        audit_fields = self._receipt_audit_fields(receipt_item)
        receipt_discriminator = self._normalize_receipt_discriminator(
            self._receipt_discriminator(receipt_item)
        )
        parsed_ok = parsed is not None and parse_error is None
        parse_method = receipt_item.get("extraction_provenance") or "structured_json"
        entry = {
            "receipt_id": (
                self._stable_receipt_id(file_record, receipt_item)
                if parsed_ok else None
            ),
            "source": {
                **self._inventory_entry(file_record),
                "page_metadata": page_metadata,
                "receipt_discriminator": receipt_discriminator,
            },
            "parse_status": "parsed" if parsed_ok else "not_detected",
            "parse_error": parse_error,
            "parse_method": parse_method if parsed_ok else None,
            "document_type": self._redact_secret_values(receipt_item.get("tipo_documento")),
            "description": self._redact_secret_values(
                receipt_item.get("description") or (parsed or {}).get("description")
            ),
            "ocr": {
                "raw_content_type": "json" if parsed is not None else "text",
                "raw_preview": self._redact_secret_values(
                    receipt_item.get("ocr_text")
                    or (raw_content[:1200] if isinstance(raw_content, str) else str(raw_content)[:1200])
                ),
            },
            "fields": fields,
            "audit_fields": audit_fields,
            **audit_fields,
            "amount_candidates": amount_candidates,
            "proposed_amount": self._proposed_amount(amount_candidates),
            "expense_category": self._category_candidates(
                receipt_item, file_record, category_catalog_snapshot
            ),
            "extraction_confidence": self._confidence_0_1(
                receipt_item.get("extraction_confidence", (parsed or {}).get("extraction_confidence"))
            ),
        }
        if audit_fields.get("provider") not in (None, ""):
            entry["proveedor"] = audit_fields["provider"]
        return entry

    def _build_receipt_batch_artifact(
        self,
        results: Dict[str, str],
        pdf_files: List[dict],
        image_files: List[dict],
        docx_files: List[dict],
        skipped: List[Tuple[str, str]],
        category_catalog_snapshot: Optional[dict] = None,
    ) -> dict:
        processed = pdf_files + docx_files + image_files
        receipts = []
        for file_record in processed:
            raw_content = results.get(file_record.get("file_uuid"), "Sin resultado")
            parsed = self._parse_extraction_content(raw_content, file_record.get("file_name", "?"))
            receipt_items, parse_error = self._receipt_items_from_parsed(parsed)
            if not receipt_items:
                receipts.append(self._receipt_entry(
                    file_record,
                    parsed,
                    None,
                    raw_content,
                    category_catalog_snapshot,
                    parse_error=parse_error,
                ))
                continue
            for receipt_item in receipt_items:
                receipts.append(self._receipt_entry(
                    file_record,
                    parsed,
                    receipt_item,
                    raw_content,
                    category_catalog_snapshot,
                ))
        receipts = self._dedupe_receipts(receipts)

        skipped_entries = [
            {"file_name": name, "status": "skipped", "reason": reason}
            for name, reason in skipped
        ]
        missing_attachments = not processed
        artifact = {
            "schema_version": ARTIFACT_SCHEMA_VERSION,
            "confidence_scale": {
                "min": 0.0,
                "max": 1.0,
                "normalization": "LLM confidences in 0..100 are divided by 100; invalid or missing confidence is 0.",
            },
            "policy": {
                "roma_writes": False,
                "approval_decisions": False,
                "secrets_redacted": True,
            },
            "batch": {
                "orchestration_session_uuid": self.orchestration_event.orchestration_session_uuid,
                "internal_orchestration_session_uuid": self.orchestration_event.internal_orchestration_session_uuid,
                "event_id": str(self.orchestration_event.event_id),
                "missing_attachments": missing_attachments,
                "processed_count": len(processed),
                "skipped_count": len(skipped),
            },
            "category_catalog": {
                "status": (category_catalog_snapshot or {}).get("status", "unresolved"),
                "version": (category_catalog_snapshot or {}).get("version"),
                "source": (category_catalog_snapshot or {}).get("source", "not_provided"),
            },
            "attachment_inventory": [
                self._inventory_entry(file_record) for file_record in processed
            ] + skipped_entries,
            "receipts": receipts,
        }
        return self._redact_secret_values(artifact)

    def _upload_receipt_batch_artifact(self, artifact: dict) -> str:
        content = json.dumps(artifact, ensure_ascii=False, indent=2).encode("utf-8")
        file_obj = io.BytesIO(content)
        file_obj.name = "pompeyo_receipt_batch.json"
        response = files_api_manager.call(
            "upload_file",
            file=file_obj,
            orchestration_session_uuids=[self.orchestration_event.orchestration_session_uuid],
            internal_orchestration_session_uuid=self.orchestration_event.internal_orchestration_session_uuid,
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
        )
        upload_data = _unwrap_response(response, "Failed to upload receipt batch artifact")
        for key in ("file_uuid", "uuid", "attachment_uuid", "id"):
            value = upload_data.get(key)
            if value:
                return str(value)
        file_data = upload_data.get("file")
        if isinstance(file_data, dict):
            for key in ("file_uuid", "uuid", "attachment_uuid", "id"):
                value = file_data.get(key)
                if value:
                    return str(value)
        raise RuntimeError("Upload response did not include a file UUID")

    def _apply_rut_dv_check(self, parsed: dict) -> None:
        """Lower confidence of RUT/RUN fields whose check digit fails módulo-11."""
        campos = parsed.get("campos_extraidos")
        if not isinstance(campos, dict):
            return
        for field_name, field_data in campos.items():
            if not isinstance(field_data, dict):
                continue
            if "rut" not in field_name.lower() and "run" not in field_name.lower():
                continue
            valor = field_data.get("valor")
            if not isinstance(valor, str) or not valor.strip():
                continue
            dv_ok = _rut_dv_valido(valor)
            if dv_ok is False:
                confianza_actual = field_data.get("confianza") or 0
                field_data["confianza"] = min(confianza_actual, 50)
                field_data["nota_dv"] = (
                    "DV inválido (módulo-11): posible error de lectura OCR; "
                    "confianza reducida automáticamente."
                )
                logger.info(
                    f"dv_check field={field_name!r} dv_invalido "
                    f"confianza->{field_data['confianza']}"
                )

    def _format_value_for_table(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value).replace("\n", " ").replace("|", "\\|").strip()

    def _build_confidence_table(self, parsed: Optional[dict]) -> str:
        if not parsed:
            return ""

        fields = parsed.get("campos_extraidos")
        if not isinstance(fields, dict):
            return ""

        rows = ["| Campo | Valor | Confianza |", "|---|---|---|"]
        for field_name, field_data in fields.items():
            if isinstance(field_data, dict):
                value = field_data.get("valor")
                confidence = field_data.get("confianza")
            else:
                value = field_data
                confidence = ""

            rows.append(
                "| "
                f"{self._format_value_for_table(field_name)} | "
                f"{self._format_value_for_table(value)} | "
                f"{self._format_value_for_table(confidence)} |"
            )

        global_confidence = parsed.get("extraction_confidence")
        if global_confidence is not None:
            rows.append(
                "| extraction_confidence | "
                "| "
                f"{self._format_value_for_table(global_confidence)} |"
            )

        return "\n".join(rows)

    def _format_file_result_section(self, title: str, content: str) -> str:
        parsed = self._parse_extraction_content(content, title)
        confidence_table = self._build_confidence_table(parsed)
        json_crudo = (
            json.dumps(parsed, ensure_ascii=False, indent=2)
            if parsed is not None else content
        )
        if not confidence_table:
            return f"{title}\nJSON crudo:\n```json\n{json_crudo}\n```"

        return (
            f"{title}\n"
            f"Tabla de confianza:\n{confidence_table}\n\n"
            f"JSON crudo:\n```json\n{json_crudo}\n```"
        )

    def _build_final_response(
        self,
        results: Dict[str, str],
        pdf_files: List[dict],
        image_files: List[dict],
        docx_files: List[dict],
        skipped: List[Tuple[str, str]],
        report: str,
        category_catalog_snapshot: Optional[dict] = None,
    ) -> str:
        artifact = self._build_receipt_batch_artifact(
            results, pdf_files, image_files, docx_files, skipped,
            category_catalog_snapshot,
        )
        ready_receipts_uuid = self._upload_receipt_batch_artifact(artifact)
        sections = []

        # PDF results
        for i, f in enumerate(pdf_files, 1):
            uuid = f["file_uuid"]
            name = f.get("file_name", uuid)
            content = results.get(uuid, "Sin resultado")
            sections.append(self._format_file_result_section(f"--- PDF {i}: {name} ---", content))

        # DOCX results
        for i, f in enumerate(docx_files, 1):
            uuid = f["file_uuid"]
            name = f.get("file_name", uuid)
            content = results.get(uuid, "Sin resultado")
            sections.append(self._format_file_result_section(f"--- DOCX {i}: {name} ---", content))

        # Image results
        for i, f in enumerate(image_files, 1):
            uuid = f["file_uuid"]
            name = f.get("file_name", uuid)
            content = results.get(uuid, "Sin resultado")
            sections.append(self._format_file_result_section(f"--- Imagen {i}: {name} ---", content))

        analysis = "\n\n".join(sections)

        response_payload = {
            "ready_receipts_uuid": ready_receipts_uuid,
            "schema_version": artifact["schema_version"],
            "batch": artifact["batch"],
            "category_catalog": artifact["category_catalog"],
            "summary": {
                "processed_count": artifact["batch"]["processed_count"],
                "skipped_count": artifact["batch"]["skipped_count"],
                "missing_attachments": artifact["batch"]["missing_attachments"],
                "receipt_count": len(artifact["receipts"]),
            },
        }
        human_summary = (
            f"Batch de boletas listo: ready_receipts_uuid={ready_receipts_uuid}. "
            f"Procesados={artifact['batch']['processed_count']}, "
            f"omitidos={artifact['batch']['skipped_count']}, "
            f"categorias={artifact['category_catalog']['status']}."
        )
        if analysis:
            logger.info("receipt_batch_detail_sections=%s", len(sections))
        logger.info("receipt_batch_processing_report=%s", report)
        return (
            f"{json.dumps(response_payload, ensure_ascii=False)}\n\n"
            f"{human_summary}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_tool_args(self) -> Dict[str, Any]:
        extra_params = self.orchestration_event.extra_params or {}
        tool_calls = extra_params.get("tool_calls", [])

        if not tool_calls:
            logger.warning("No tool calls found in orchestration event")
            return {}

        return tool_calls[0].get("args", {})

    def _get_model(self) -> str:
        logger.info(f"Using primary extraction model: {EXTRACTION_PRIMARY_MODEL}")
        return EXTRACTION_PRIMARY_MODEL

    def _get_openai_api_key(self) -> str:
        extra_params = self.orchestration_event.extra_params or {}
        openai_api_key = extra_params.get("openai_api_key")

        if openai_api_key:
            logger.info("Using OpenAI API key from extra_params")
            return openai_api_key

        from chask_foundation.configs.global_config import get_openai_api_key
        openai_api_key = get_openai_api_key()
        logger.info("Using OpenAI API key from AWS Secrets Manager")
        return openai_api_key

    def _get_gemini_api_key(self) -> Optional[str]:
        global _gemini_key_cache, _gemini_key_fetched
        if _gemini_key_fetched:
            return _gemini_key_cache
        _gemini_key_fetched = True
        try:
            from chask_foundation.configs.utils import get_secret
            mode = os.getenv("MODE", "PRODUCTION")
            secret_str = get_secret("chask/gemini", MODE=mode)
            api_key = json.loads(secret_str).get("GEMINI_API_KEY")
            if not api_key:
                logger.warning("GEMINI_API_KEY not found in chask/api-secrets")
                return None
            os.environ["GEMINI_API_KEY"] = api_key
            _gemini_key_cache = api_key
            logger.info("GEMINI_API_KEY loaded from chask/api-secrets")
            return api_key
        except Exception as e:
            logger.warning(f"Could not load GEMINI_API_KEY: {e}")
            return None

    def _is_ensemble_enabled(self) -> bool:
        return ENABLE_GEMINI_ENSEMBLE

    def _parse_llm_json(self, content: str) -> Optional[dict]:
        if not isinstance(content, str) or not content.strip():
            return None
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _strip_accents_casefold(self, value: str) -> str:
        decomposed = unicodedata.normalize("NFKD", value)
        without_marks = "".join(
            ch for ch in decomposed if not unicodedata.combining(ch)
        )
        return without_marks.casefold()

    def _normalize_name_tokens(self, value: Any) -> Optional[List[str]]:
        if not isinstance(value, str):
            return None
        normalized = self._strip_accents_casefold(value)
        tokens = re.findall(r"[a-z0-9]+", normalized)
        return sorted(tokens) if tokens else None

    def _is_name_like_field(self, field_name: str) -> bool:
        normalized = self._strip_accents_casefold(field_name)
        return any(
            marker in normalized
            for marker in ("nombre", "apellido", "razon", "social", "titular")
        )

    def _parse_date_value(self, value: Any) -> Optional[date]:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None

        numeric = re.search(r"\b(\d{1,4})[./-](\d{1,2})[./-](\d{1,4})\b", text)
        if numeric:
            a, b, c = (int(part) for part in numeric.groups())
            candidates = []
            if a > 31:
                candidates.append((a, b, c))
            elif c > 31:
                candidates.append((c, b, a))
            else:
                candidates.extend(((c, b, a), (a, b, c)))
            for year, month, day in candidates:
                if year < 100:
                    year += 2000
                try:
                    return datetime(year, month, day).date()
                except ValueError:
                    continue

        iso = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", text)
        if iso:
            year, month, day = (int(part) for part in iso.groups())
            try:
                return datetime(year, month, day).date()
            except ValueError:
                return None
        return None

    def _parse_numeric_value(self, value: Any) -> Optional[float]:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if not isinstance(value, str):
            return None

        text = value.strip().casefold()
        if not text:
            return None
        if re.search(r"\b(cero|sin\s+(monto|valor|cargo|costo)|no\s+aplica|n/?a|ninguno)\b", text):
            return 0.0

        matches = re.findall(r"[-+]?\d[\d.,\s]*", text)
        if not matches:
            return None
        raw = max(matches, key=len).strip().replace(" ", "")
        if not raw:
            return None

        comma = raw.rfind(",")
        dot = raw.rfind(".")
        if comma != -1 and dot != -1:
            decimal_sep = "," if comma > dot else "."
            thousands_sep = "." if decimal_sep == "," else ","
            normalized = raw.replace(thousands_sep, "").replace(decimal_sep, ".")
        elif comma != -1:
            normalized = self._normalize_single_separator_number(raw, ",")
        elif dot != -1:
            normalized = self._normalize_single_separator_number(raw, ".")
        else:
            normalized = raw

        try:
            return float(normalized)
        except ValueError:
            return None

    def _normalize_single_separator_number(self, raw: str, sep: str) -> str:
        parts = raw.split(sep)
        if len(parts) == 2 and len(parts[1]) in (1, 2):
            return raw.replace(sep, ".")
        if len(parts) > 1 and all(len(part) == 3 for part in parts[1:]):
            return "".join(parts)
        return raw.replace(sep, ".")

    def _values_equivalent(self, field_name: str, value_a: Any, value_b: Any) -> bool:
        if value_a == value_b:
            return True
        if value_a is None or value_b is None:
            return False

        date_a = self._parse_date_value(value_a)
        date_b = self._parse_date_value(value_b)
        if date_a and date_b:
            return date_a == date_b

        number_a = self._parse_numeric_value(value_a)
        number_b = self._parse_numeric_value(value_b)
        if number_a is not None and number_b is not None:
            return abs(number_a - number_b) < 0.000001

        if self._is_name_like_field(field_name):
            tokens_a = self._normalize_name_tokens(value_a)
            tokens_b = self._normalize_name_tokens(value_b)
            if tokens_a and tokens_b:
                return tokens_a == tokens_b

        if isinstance(value_a, str) and isinstance(value_b, str):
            text_a = re.sub(r"\W+", "", self._strip_accents_casefold(value_a))
            text_b = re.sub(r"\W+", "", self._strip_accents_casefold(value_b))
            return bool(text_a) and text_a == text_b

        return False

    def _apply_equivalence_normalization(
        self,
        judge_content: str,
        primary_content: str,
        gemini_content: str,
    ) -> str:
        judged = self._parse_llm_json(judge_content)
        primary = self._parse_llm_json(primary_content)
        gemini = self._parse_llm_json(gemini_content)
        if not judged or not primary or not gemini:
            return judge_content

        judged_fields = judged.get("campos_extraidos")
        primary_fields = primary.get("campos_extraidos")
        gemini_fields = gemini.get("campos_extraidos")
        if not all(isinstance(fields, dict) for fields in (
            judged_fields, primary_fields, gemini_fields
        )):
            return judge_content

        changed = False
        for field_name, judged_field in judged_fields.items():
            if not isinstance(judged_field, dict):
                continue
            primary_field = primary_fields.get(field_name)
            gemini_field = gemini_fields.get(field_name)
            if not isinstance(primary_field, dict) or not isinstance(gemini_field, dict):
                continue

            value_a = primary_field.get("valor")
            value_b = gemini_field.get("valor")
            if not self._values_equivalent(field_name, value_a, value_b):
                continue

            conf_a = primary_field.get("confianza")
            conf_b = gemini_field.get("confianza")
            if not isinstance(conf_a, (int, float)) or not isinstance(conf_b, (int, float)):
                continue

            equivalent_confidence = int(min(conf_a, conf_b, 99))
            current_confidence = judged_field.get("confianza")
            if (
                isinstance(current_confidence, (int, float))
                and equivalent_confidence > current_confidence
            ):
                judged_field["confianza"] = equivalent_confidence
                changed = True

            if judged_field.get("valor") in (None, ""):
                judged_field["valor"] = value_a
                changed = True

            if "nota_discrepancia" in judged_field:
                judged_field.pop("nota_discrepancia", None)
                changed = True

            logger.info(
                f"equivalence_normalization field={field_name!r} "
                f"confianza={judged_field.get('confianza')}"
            )

        if not changed:
            return judge_content
        return json.dumps(judged, ensure_ascii=False)

    def _chat_with_ensemble(
        self,
        llm_client: Any,
        messages: list,
        caller_function: str,
        extraction_context: Optional[str] = None,
        temperature: float = 1,
        response_format: Optional[dict] = None,
    ) -> dict:
        """
        3-stage ensemble: gpt-5.5 (primary) + gemini in parallel, then judge nano reconciles.
        Falls back to gpt-5.5 alone if gemini or judge fail.
        Returns a dict compatible with llm_client.chat() output — transparent to callers.
        """
        if not self._is_ensemble_enabled():
            return llm_client.chat(
                messages=messages,
                temperature=temperature,
                response_format=response_format,
                caller_function=caller_function,
            )

        gemini_key = self._get_gemini_api_key()
        if not gemini_key:
            return llm_client.chat(
                messages=messages,
                temperature=temperature,
                response_format=response_format,
                caller_function=caller_function,
            )

        # Stage 1+2: gpt-5.5 and gemini in parallel
        with ThreadPoolExecutor(max_workers=2) as exc:
            primary_future = exc.submit(
                llm_client.chat,
                messages=messages,
                temperature=temperature,
                response_format=response_format,
                caller_function=caller_function,
            )
            gemini_future = exc.submit(
                llm_client.chat,
                messages=messages,
                model="gemini/gemini-3.1-pro-preview",
                temperature=temperature,
                response_format=response_format,
                caller_function=f"{caller_function}_gemini",
            )
            primary_response = primary_future.result()
            try:
                gemini_response = gemini_future.result()
            except Exception as e:
                logger.warning(
                    f"ensemble_gemini_failed caller={caller_function} error={e}; "
                    "falling back to gpt-5.5"
                )
                return primary_response

        if not primary_response.get("success", True):
            return primary_response

        if not gemini_response.get("success", True):
            logger.warning(
                f"ensemble_gemini_error caller={caller_function} "
                f"error={gemini_response.get('error')}; falling back to gpt-5.5"
            )
            return primary_response

        # Stage 3: judge reconciles (text-only, no images)
        context_block = ""
        if extraction_context:
            context_block = (
                "Contexto/instrucciones originales de extracción usadas por ambos "
                f"modelos:\n{extraction_context.strip()}\n\n"
            )
        judge_user = (
            f"{context_block}"
            f"Modelo A ({EXTRACTION_PRIMARY_MODEL}):\n{primary_response.get('content', '')}\n\n"
            f"Modelo B (gemini-3.1-pro-preview):\n{gemini_response.get('content', '')}"
        )
        try:
            judge_response = llm_client.chat(
                messages=[
                    {"role": "system", "content": ENSEMBLE_JUDGE_SYSTEM_PROMPT},
                    {"role": "user", "content": judge_user},
                ],
                model=ENSEMBLE_JUDGE_MODEL,
                temperature=1,
                response_format={"type": "json_object"},
                caller_function=f"{caller_function}_judge",
            )
            if not judge_response.get("success", True) or not judge_response.get("content"):
                logger.warning(
                    f"ensemble_judge_failed caller={caller_function} "
                    f"error={judge_response.get('error')}; falling back to gpt-5.5"
                )
                return primary_response
            normalized_content = self._apply_equivalence_normalization(
                judge_response["content"],
                primary_response.get("content", ""),
                gemini_response.get("content", ""),
            )
            logger.info(f"ensemble_judge_ok caller={caller_function}")
            return {**primary_response, "content": normalized_content}
        except Exception as e:
            logger.warning(
                f"ensemble_judge_exception caller={caller_function} error={e}; "
                "falling back to gpt-5.5"
            )
            return primary_response
