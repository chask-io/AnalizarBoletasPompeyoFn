from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from backend.pompeyo_rules import (
    POMPEYO_EXTRACTION_RULES,
    canonical_financial_field,
    normalize_financial_candidates,
    split_clp_and_uf,
)
ROOT = Path(__file__).resolve().parents[1]


def test_runtime_sources_compile():
    for relative in ("src/backend/function_logic.py", "src/backend/pompeyo_rules.py", "src/handler.py"):
        source = (ROOT / relative).read_text(encoding="utf-8")
        compile(source, relative, "exec")


def test_no_contradictory_expiry_or_zero_equivalence_rules():
    source = (ROOT / "src/backend/function_logic.py").read_text(encoding="utf-8")
    assert "fecha_vencimiento es la POSTERIOR" not in source
    assert "dinero cero expresado como $0, 0, sin monto, no aplica" not in source
    assert "Cero, null, ausente y no aplica NO son equivalentes" in source


def test_forum_total_payment_is_preserved_separately():
    assert "Cuota mensual -> cuota_mensual_base" in POMPEYO_EXTRACTION_RULES
    assert "Desgravamen -> desgravamen" in POMPEYO_EXTRACTION_RULES
    assert "Cuota mensual a pagar -> cuota_mensual_a_pagar y valor_cuota" in POMPEYO_EXTRACTION_RULES


def test_financing_candidates_keep_semantics():
    for label in (
        "total_a_financiar",
        "monto_liquido_credito",
        "saldo_a_financiar",
        "saldo_valor_vehiculo",
        "saldo_precio",
    ):
        assert label in POMPEYO_EXTRACTION_RULES
    assert "No colapses ni elijas" in POMPEYO_EXTRACTION_RULES


def test_extractor_never_decides_business_state():
    assert "NO decidas APROBADO, OBSERVADO, RECHAZADO" in POMPEYO_EXTRACTION_RULES


def test_manifest_is_pompeyo_specific():
    manifest = (ROOT / "manifest.yml").read_text(encoding="utf-8")
    assert "name: AnalizarBoletasPompeyoFn" in manifest


def test_216839_candidate_labels_override_incorrect_model_normalization():
    parsed = {
        "campos_extraidos": {},
        "candidatos_financiamiento_por_pagina": [
            {"etiqueta": "Cuota mensual ($)", "campo_normalizado": "valor_cuota", "valor": 409786, "confianza": 99},
            {"etiqueta": "Desgravamen ($)", "campo_normalizado": "valor_cuota", "valor": 13741, "confianza": 99},
            {"etiqueta": "Cuota mensual a pagar ($)", "campo_normalizado": "valor_cuota", "valor": 423527, "confianza": 99},
            {"etiqueta": "Saldo de precio ($)", "campo_normalizado": "saldo_a_financiar", "valor": 2280000, "confianza": 99},
            {"etiqueta": "Total a financiar", "valor": 9208322, "confidence_score": "98"},
        ],
    }

    normalize_financial_candidates(parsed)

    candidates = parsed["candidatos_financiamiento_por_pagina"]
    assert [candidate["campo_normalizado"] for candidate in candidates] == [
        "cuota_mensual_base",
        "desgravamen",
        "cuota_mensual_a_pagar",
        "saldo_precio",
        "total_a_financiar",
    ]
    assert parsed["campos_extraidos"]["valor_cuota"]["valor"] == 423527
    assert parsed["campos_extraidos"]["cuota_mensual_a_pagar"]["valor"] == 423527


def test_primary_financing_labels_remain_distinct():
    assert canonical_financial_field("Total a financiar") == "total_a_financiar"
    assert canonical_financial_field("Saldo valor vehículo") == "saldo_valor_vehiculo"
    assert canonical_financial_field("Saldo a financiar") == "saldo_a_financiar"



def test_217989_splits_total_financing_clp_and_uf():
    parsed = {
        "campos_extraidos": {
            "total_a_financiar": {"valor": "13.741.383 UF 336,43", "confianza": 98},
        },
        "candidatos_financiamiento_por_pagina": [
            {
                "etiqueta": "Total a financiar ($)",
                "campo_normalizado": "total_a_financiar",
                "valor": "13.741.383 UF 336,43",
                "confianza": 98,
                "texto_contexto": "Total a financiar ($) 13.741.383 UF 336,43",
            },
            {
                "etiqueta": "Total a financiar ($)",
                "campo_normalizado": "total_a_financiar",
                "valor": "336,43",
                "confianza": 98,
                "texto_contexto": "Total a financiar ($) 13.741.383 UF 336,43",
            },
        ],
    }
    normalize_financial_candidates(parsed)
    assert split_clp_and_uf("$13.741.383 UF 336,43") == ("13.741.383", "336,43")
    assert parsed["campos_extraidos"]["total_a_financiar"]["valor"] == "13.741.383"
    assert parsed["campos_extraidos"]["total_a_financiar_uf"]["valor"] == "336,43"
    candidate = parsed["candidatos_financiamiento_por_pagina"][0]
    assert candidate["valor"] == "13.741.383"
    assert candidate["valor_uf"] == "336,43"
    uf_candidate = parsed["candidatos_financiamiento_por_pagina"][1]
    assert uf_candidate["valor"] == "336,43"
    assert uf_candidate["campo_normalizado"] == "total_a_financiar_uf"
