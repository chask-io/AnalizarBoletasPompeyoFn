"""Canonical extraction contract for Pompeyo documents.

This module contains extraction rules only. It must never decide approval,
rejection, checklist marks, or business exceptions.
"""

import re
import unicodedata
from typing import Any, Optional


def _normalized_label(value: str) -> str:
    text = unicodedata.normalize("NFD", value.lower())
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def canonical_financial_field(label: Any) -> Optional[str]:
    """Return the canonical field dictated by a document visible label."""
    if not isinstance(label, str) or not label.strip():
        return None
    normalized = _normalized_label(label)
    rules = (
        (("cuota mensual a pagar",), "cuota_mensual_a_pagar"),
        (("desgravamen",), "desgravamen"),
        (("cuota mensual",), "cuota_mensual_base"),
        (("saldo de precio", "saldo precio"), "saldo_precio"),
        (("saldo valor vehiculo", "saldo del valor vehiculo"), "saldo_valor_vehiculo"),
        (("total a financiar",), "total_a_financiar"),
        (("monto total financiado",), "monto_total_financiado"),
        (("monto liquido del credito", "monto liquido credito"), "monto_liquido_credito"),
        (("monto a financiar",), "monto_a_financiar"),
        (("saldo a financiar",), "saldo_a_financiar"),
        (("valor cuota", "valor de cuota"), "valor_cuota"),
    )
    for labels, field_name in rules:
        if any(candidate in normalized for candidate in labels):
            return field_name
    return None


def split_clp_and_uf(value: Any) -> Optional[tuple[str, str]]:
    """Split a combined CLP/UF display without merging both numeric values."""
    if not isinstance(value, str) or "uf" not in value.lower():
        return None
    match = re.search(
        r"^\s*\$?\s*([0-9]{1,3}(?:\.[0-9]{3})+|[0-9]{4,})"
        r"\s*(?:CLP)?\s*(?:[/(-]\s*)?UF\s*[:=]?\s*"
        r"([0-9]+(?:[.,][0-9]+)?)",
        value,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1), match.group(2)


def normalize_financial_candidates(parsed: dict) -> None:
    """Normalize candidate fields from labels without choosing business comparators."""
    payable_candidates = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            label = value.get("etiqueta") or value.get("label")
            canonical = canonical_financial_field(label)
            raw_candidate = value.get("valor")
            context = str(value.get("texto_contexto") or "")
            if (
                canonical == "total_a_financiar"
                and isinstance(raw_candidate, str)
                and re.fullmatch(r"\s*[0-9]{1,3}[.,][0-9]+\s*", raw_candidate)
                and re.search(r"\bUF\b", context, flags=re.IGNORECASE)
            ):
                canonical = "total_a_financiar_uf"
            if canonical:
                value["campo_normalizado"] = canonical
                split_value = split_clp_and_uf(raw_candidate)
                if split_value and canonical in {
                    "total_a_financiar",
                    "monto_total_financiado",
                    "monto_a_financiar",
                    "saldo_a_financiar",
                }:
                    value["valor"], value["valor_uf"] = split_value
                if canonical == "cuota_mensual_a_pagar":
                    payable_candidates.append(value)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(parsed)

    def confidence(item: dict) -> float:
        raw = item.get("confianza", item.get("confidence_score", 0))
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    fields = parsed.get("campos_extraidos")
    if not isinstance(fields, dict):
        return
    for field_name in (
        "total_a_financiar",
        "monto_total_financiado",
        "monto_a_financiar",
        "saldo_a_financiar",
    ):
        field = fields.get(field_name)
        if not isinstance(field, dict):
            continue
        split_value = split_clp_and_uf(field.get("valor"))
        if split_value:
            field["valor"], uf_value = split_value
            fields[f"{field_name}_uf"] = {
                "valor": uf_value,
                "confianza": field.get("confianza"),
            }

    if payable_candidates:
        best = max(payable_candidates, key=confidence)
        extracted = {
            "valor": best.get("valor"),
            "confianza": best.get("confianza", best.get("confidence_score")),
        }
        fields["cuota_mensual_a_pagar"] = dict(extracted)
        fields["valor_cuota"] = dict(extracted)


POMPEYO_EXTRACTION_RULES = r"""
REGLAS CANONICAS POMPEYO DE EXTRACCION - MAYOR PRIORIDAD

RESPONSABILIDAD
Clasifica documentos y extrae hechos. NO decidas APROBADO, OBSERVADO, RECHAZADO; NO emitas [x], [✓], [⚠], [N/A], acciones requeridas ni conclusiones de match.

BOLETAS Y RENDICIONES PIPELINE 81353
- La salida es evidencia auditable para rendicion de gastos, no una decision.
- Preserva proveedor/comercio, RUT proveedor si aparece, folio/numero, fecha_emision, monto_neto, iva, monto_total, moneda, medio_pago, items visibles y texto OCR relevante.
- Si un PDF tiene varias paginas o una imagen contiene mas de un comprobante, conserva candidatos por pagina/comprobante. No colapses totales ambiguos.
- Cada candidato de monto debe conservar {archivo_fuente, pagina, etiqueta, valor, moneda, confianza, texto_contexto} cuando exista.
- Las categorias de gasto son candidatas. Usa schema {id, name, source, confidence, candidates}; si hay empate o evidencia insuficiente, marca ambiguedad explicitamente en candidates/source. No fuerces una categoria unica.
- La confianza que recibe el pipeline se normaliza luego a 0..1. Si respondes en escala 0..100, mantenla calibrada: 100 solo si el dato es nitido, visible y sin ambiguedad.
- Nunca incluyas tokens, claves, URLs firmadas ni secretos en campos extraidos u observaciones.

INVENTARIO
Preserva tipo declarado por ROMA y tipo detectado. RVM, padrón, VI, SOAP, permiso de circulación y certificados vehiculares nunca son cédula. Si un archivo declarado cédula contiene esos datos, usa tipo_documento=certificado_vehiculo_rvm_vi, documento_no_es_cedula=true y extrae propietario/RUT/patente/VIN disponibles.

IDENTIDAD
- Cédula: nombre_completo, run_rut, fecha_emision y fecha_vencimiento separados.
- fecha_vencimiento solo puede salir de una etiqueta explícita FECHA DE VENCIMIENTO/VENCIMIENTO. No la elijas por ser la fecha posterior. Si la etiqueta no es visible, valor=null y confianza baja/0.
- eRUT/ROL/SII: razon_social, rut_empresa, representante_nombre y representante_rut solo cuando estén explícitos. No afirmar que sustituye una cédula.
- Documento migratorio: titular, identificador, tipo, vigencia y estado visible.

CANDIDATOS FINANCIEROS
Preserva cada candidato con {archivo_fuente, pagina, etiqueta, campo_normalizado, valor, confianza, texto_contexto}. No colapses ni elijas el valor que deba compararse con ROMA. Mantén separados:
- total_a_financiar;
- monto_total_financiado;
- monto_liquido_credito;
- monto_a_financiar;
- saldo_a_financiar;
- saldo_valor_vehiculo;
- saldo_precio;
- precio;
- colocacion;
- pie;
- VFG;
- total_pagare;
- solicitud_inscripcion.
Nunca renombres Saldo valor vehículo o Saldo precio como Total a financiar.
Si una línea muestra pesos y UF juntos, por ejemplo `13.741.383 UF 336,43`, extrae
`total_a_financiar=13.741.383` y `total_a_financiar_uf=336,43` por separado. Nunca
concatenes ambos números ni produzcas `13.741.383.336`.

FORUM VALOR CUOTA
Si aparecen:
- Cuota mensual -> cuota_mensual_base;
- Desgravamen -> desgravamen;
- Cuota mensual a pagar -> cuota_mensual_a_pagar y valor_cuota.
Conserva los tres en candidatos_financiamiento_por_pagina. El valor_cuota principal extraído es Cuota mensual a pagar, no la cuota base. Ejemplo: 409.786 + 13.741 = 423.527.

CUOTAS Y CUOTON
Preserva cuotas_base, documento_total_cuotas, presencia_cuoton, VFG/cuota_final. Si el documento muestra base y total, no reemplaces uno por otro. El total incluye el cuotón solo cuando la estructura lo demuestra.

VALORES AUSENTES Y CONFIANZA
Cero, null, ausente y N/A son valores distintos. No los declares equivalentes. Nunca asumir confianza >=90 cuando falta metadata. Documento adjunto ilegible es distinto de documento ausente.
"""
