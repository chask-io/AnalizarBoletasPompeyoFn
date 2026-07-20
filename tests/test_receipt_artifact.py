import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def _install_chask_stubs():
    chask_foundation = types.ModuleType("chask_foundation")
    backend_mod = types.ModuleType("chask_foundation.backend")
    models_mod = types.ModuleType("chask_foundation.backend.models")
    api_mod = types.ModuleType("chask_foundation.api")
    files_mod = types.ModuleType("chask_foundation.api.files_requests")
    pipeline_mod = types.ModuleType("chask_foundation.api.pipeline_requests")
    top_api_mod = types.ModuleType("api")
    top_files_mod = types.ModuleType("api.files_requests")
    top_pipeline_mod = types.ModuleType("api.pipeline_requests")

    class OrchestrationEvent:
        pass

    class DummyManager:
        def call(self, *args, **kwargs):
            raise AssertionError("network/API calls are not allowed in artifact unit tests")

    models_mod.OrchestrationEvent = OrchestrationEvent
    files_mod.files_api_manager = DummyManager()
    pipeline_mod.pipeline_api_manager = DummyManager()
    top_files_mod.files_api_manager = DummyManager()
    top_pipeline_mod.pipeline_api_manager = DummyManager()

    sys.modules.setdefault("chask_foundation", chask_foundation)
    sys.modules.setdefault("chask_foundation.backend", backend_mod)
    sys.modules.setdefault("chask_foundation.backend.models", models_mod)
    sys.modules.setdefault("chask_foundation.api", api_mod)
    sys.modules.setdefault("chask_foundation.api.files_requests", files_mod)
    sys.modules.setdefault("chask_foundation.api.pipeline_requests", pipeline_mod)
    sys.modules.setdefault("api", top_api_mod)
    sys.modules.setdefault("api.files_requests", top_files_mod)
    sys.modules.setdefault("api.pipeline_requests", top_pipeline_mod)


_install_chask_stubs()

from backend import function_logic
from backend.function_logic import ARTIFACT_SCHEMA_VERSION, FunctionBackend


def _backend():
    backend = FunctionBackend.__new__(FunctionBackend)
    backend.orchestration_event = SimpleNamespace(
        orchestration_session_uuid="session-uuid",
        internal_orchestration_session_uuid="internal-session-uuid",
        event_id="event-uuid",
        access_token="test-token",
        organization=SimpleNamespace(organization_id="org-uuid"),
    )
    backend._source_bytes_cache = {}
    return backend


def _file(uuid="file-1", name="boleta.pdf", mime="application/pdf"):
    return {
        "file_uuid": uuid,
        "file_name": name,
        "mime_type": mime,
        "size": 1234,
        "content_bytes": b"immutable receipt bytes",
    }


def test_multi_page_amount_candidates_preserve_page_metadata_and_confidence_scale():
    backend = _backend()
    parsed = {
        "campos_extraidos": {
            "total_pagina_1": {"valor": "$10.000", "confianza": 80, "pagina": 1},
            "total_pagina_2": {"valor": "$12.000", "confianza": 60, "pagina": 2},
        },
        "extraction_confidence": 70,
    }

    candidates = backend._amount_candidates(parsed, _file())

    assert [candidate["source"]["page"] for candidate in candidates] == [1, 2]
    assert [candidate["confidence"] for candidate in candidates] == [0.8, 0.6]
    assert candidates[0]["currency"] == "CLP"
    assert candidates[0]["source"]["source_content_sha256"]


def test_missing_attachments_emit_batch_artifact_instead_of_special_case_text():
    backend = _backend()

    artifact = backend._build_receipt_batch_artifact({}, [], [], [], [("N/A", "No hay archivos en la sesion")])

    assert artifact["schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert artifact["batch"]["missing_attachments"] is True
    assert artifact["attachment_inventory"][0]["status"] == "skipped"
    assert artifact["receipts"] == []


def test_malformed_ocr_is_marked_without_throwing():
    backend = _backend()

    receipt = backend._receipt_entry(
        _file(), None, None, "not-json-response",
        parse_error="malformed_or_non_json_extraction",
    )

    assert receipt["parse_status"] == "not_detected"
    assert receipt["parse_error"] == "malformed_or_non_json_extraction"
    assert receipt["ocr"]["raw_content_type"] == "text"


def test_unresolved_category_catalog_does_not_emit_guessed_ids():
    backend = _backend()
    parsed = {
        "description": "Compra en Copec con pago de estacionamiento",
        "campos_extraidos": {
            "proveedor": {"valor": "Copec Parking", "confianza": 95},
        },
    }

    category = backend._category_candidates(parsed, _file())

    assert category["id"] is None
    assert category["name"] is None
    assert category["status"] == "unresolved"
    assert category["ambiguous"] is True
    assert {"id", "name", "source", "confidence", "candidates"} <= set(category)
    assert category["candidates"] == []


def test_injected_category_snapshot_can_resolve_candidates_but_keeps_ambiguity():
    backend = _backend()
    snapshot = backend._parse_category_catalog_snapshot({
        "version": "roma-readonly-2026-07-20",
        "source": "safe_read_only_roma_discovery",
        "categories": [
            {"id": 10, "name": "Combustible", "keywords": ["copec"]},
            {"id": 11, "name": "Estacionamiento", "keywords": ["parking"]},
        ],
    })
    parsed = {"description": "Compra Copec Parking"}

    category = backend._category_candidates(parsed, _file(), snapshot)

    assert category["status"] == "resolved"
    assert category["catalog"]["version"] == "roma-readonly-2026-07-20"
    assert category["id"] in {"10", "11"}
    assert category["ambiguous"] is True
    assert len(category["candidates"]) == 2


def test_proposed_amount_prefers_total_label_deterministically():
    backend = _backend()
    parsed = {
        "campos_extraidos": {
            "monto_neto": {"valor": "$8.403", "confianza": 95},
            "monto_total": {"valor": "$10.000", "confianza": 80},
        }
    }

    receipt = backend._receipt_entry(_file(), parsed, parsed, json.dumps(parsed))

    assert receipt["proposed_amount"]["numeric_value"] == 10000
    assert receipt["proposed_amount"]["candidate_id"] == "file-1:monto_total"
    assert receipt["expense_category"]["status"] == "unresolved"


def test_scenario_a_synthetic_monto_shape_becomes_actionable():
    backend = _backend()
    parsed = json.loads((ROOT / "test_files/scenario_a_synthetic_extraction.json").read_text(encoding="utf-8"))
    file_uuid = "14c1f293-9135-4a92-b127-cf62cb664744"

    artifact = backend._build_receipt_batch_artifact(
        {file_uuid: json.dumps(parsed)},
        [_file(uuid=file_uuid, name="synthetic_receipt_demo_81353.png", mime="image/png")],
        [],
        [],
        [],
    )

    receipt = artifact["receipts"][0]
    assert receipt["parse_status"] == "parsed"
    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["proposed_amount"]["currency"] == "CLP"
    assert receipt["proposed_amount"]["candidate_id"].endswith(":monto")
    assert receipt["proposed_amount"]["selection_rule"] == "prefer_total_label_then_confidence"
    assert receipt["proposed_amount"]["ambiguous"] is False
    assert receipt["amount_candidates"][1]["source"]["context"] == "TOTAL CLP $18.750"


def test_final_acceptance_truncated_preview_recovers_actionable_receipt():
    backend = _backend()
    raw_preview = (ROOT / "test_files/final_acceptance_truncated_preview_767aefb6.json").read_text(
        encoding="utf-8"
    )
    file_uuid = "14c1f293-9135-4a92-b127-cf62cb664744"
    catalog = backend._parse_category_catalog_snapshot({
        "version": "simulation-81353",
        "source": "synthetic_simulation_only",
        "categories": [
            {
                "id": "SIM-ALIMENTACION-81353",
                "name": "ALIMENTACIÓN",
                "keywords": ["ALIMENTACIÓN", "ALIMENTACION"],
            }
        ],
    })

    artifact = backend._build_receipt_batch_artifact(
        {file_uuid: raw_preview},
        [_file(uuid=file_uuid, name="synthetic_receipt_demo_81353.png", mime="image/png")],
        [],
        [],
        [],
        catalog,
    )

    assert artifact["batch"]["processed_count"] == 1
    assert len(artifact["receipts"]) == 1
    receipt = artifact["receipts"][0]
    assert receipt["parse_status"] == "parsed"
    assert receipt["parse_error"] is None
    assert receipt["parse_method"] == "deterministic_text_fallback"
    assert receipt["receipt_id"]
    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["proposed_amount"]["currency"] == "CLP"
    assert receipt["proposed_amount"]["ambiguous"] is False
    assert receipt["expense_category"]["id"] == "SIM-ALIMENTACION-81353"
    assert receipt["expense_category"]["status"] == "resolved"
    assert receipt["fields"][0]["confidence"] <= 0.72


def test_ready_receipts_structured_json_is_not_discarded():
    backend = _backend()
    file_uuid = "14c1f293-9135-4a92-b127-cf62cb664744"
    parsed = {
        "ready_receipts": [
            {
                "parse_status": "ok",
                "monto": {"valor": 18750, "moneda": "CLP", "confianza": 99},
                "fecha": {"valor": "2026-07-20", "confianza": 99},
                "proveedor": {"valor": "RESTAURANTE DEMO POMPEYO", "confianza": 99},
                "numero_folio": {"valor": "DEMO-81353", "confianza": 99},
                "categoria": {
                    "category_id": "SIM-ALIMENTACION-81353",
                    "name": "ALIMENTACIÓN",
                    "confidence": 0.99,
                },
            }
        ]
    }
    catalog = backend._parse_category_catalog_snapshot({
        "version": "simulation-81353",
        "source": "synthetic_simulation_only",
        "categories": [
            {
                "id": "SIM-ALIMENTACION-81353",
                "name": "ALIMENTACIÓN",
                "keywords": ["ALIMENTACIÓN"],
            }
        ],
    })

    artifact = backend._build_receipt_batch_artifact(
        {file_uuid: json.dumps(parsed)},
        [_file(uuid=file_uuid, name="synthetic_receipt_demo_81353.png", mime="image/png")],
        [],
        [],
        [],
        catalog,
    )

    receipt = artifact["receipts"][0]
    assert receipt["parse_status"] == "parsed"
    assert receipt["parse_method"] == "structured_json"
    assert receipt["parse_error"] is None
    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["expense_category"]["id"] == "SIM-ALIMENTACION-81353"


def test_v5_proveedor_razon_social_populates_provider_contract_fields():
    backend = _backend()
    file_uuid = "14c1f293-9135-4a92-b127-cf62cb664744"
    evidence_image = Path("/tmp/pompeyo-81353-v5-contract-failure/synthetic_receipt_demo_81353.png")
    if not evidence_image.exists():
        pytest.skip("sanitized v5 evidence image is not present")
    file_record = _file(
        uuid=file_uuid,
        name="synthetic_receipt_demo_81353.png",
        mime="image/png",
    )
    file_record["content_bytes"] = evidence_image.read_bytes()
    parsed = {
        "receipts": [
            {
                "receipt_discriminator": "demo-81353-restaurante-demo-pompeyo-2026-07-20-18750",
                "page_metadata": {
                    "page_index": 1,
                    "page_range": [1, 1],
                    "group_label": "boleta 1",
                },
                "tipo_documento": "boleta",
                "description": "Boleta electronica de restaurante demo con total en CLP y categoria sugerida alimentacion.",
                "ocr_text": (
                    "DOCUMENTO FICTICIO\n"
                    "RESTAURANTE DEMO POMPEYO\n"
                    "RUT: 00.000.000-0\n"
                    "FOLIO: DEMO-81353\n"
                    "FECHA: 20-07-2026\n"
                    "TOTAL CLP $18.750\n"
                    "CATEGORIA SUGERIDA: ALIMENTACION"
                ),
                "campos_extraidos": {
                    "proveedor_razon_social": {
                        "valor": "RESTAURANTE DEMO POMPEYO",
                        "confianza": 99,
                        "pagina": 1,
                    },
                    "rut_proveedor": {"valor": "00.000.000-0", "confianza": 99, "pagina": 1},
                    "folio": {"valor": "DEMO-81353", "confianza": 99, "pagina": 1},
                    "fecha": {"valor": "2026-07-20", "confianza": 99, "pagina": 1},
                    "moneda": {"valor": "CLP", "confianza": 98, "pagina": 1},
                    "monto_total": {"valor": "18750", "confianza": 99, "pagina": 1},
                    "categoria": {
                        "valor": {
                            "id": "SIM-ALIMENTACION-81353",
                            "name": "ALIMENTACION",
                            "confidence": 0.99,
                            "ambiguous": False,
                            "candidates": [
                                {
                                    "id": "SIM-ALIMENTACION-81353",
                                    "name": "ALIMENTACION",
                                    "confidence": 0.99,
                                }
                            ],
                        },
                        "confianza": 99,
                        "pagina": 1,
                    },
                },
                "extraction_confidence": 99,
            }
        ],
        "extraction_confidence": 99,
    }
    catalog = backend._parse_category_catalog_snapshot({
        "version": "simulation-81353",
        "source": "synthetic_simulation_only",
        "categories": [
            {
                "id": "SIM-ALIMENTACION-81353",
                "name": "ALIMENTACION",
                "keywords": ["RESTAURANTE", "ALIMENTACION", "DEMO POMPEYO"],
            }
        ],
    })

    artifact = backend._build_receipt_batch_artifact(
        {file_uuid: json.dumps(parsed, ensure_ascii=False)},
        [],
        [file_record],
        [],
        [],
        catalog,
    )

    assert artifact["batch"]["processed_count"] == 1
    assert len(artifact["receipts"]) == 1
    receipt = artifact["receipts"][0]
    assert receipt["parse_status"] == "parsed"
    assert receipt["provider"] == "RESTAURANTE DEMO POMPEYO"
    assert receipt["proveedor"] == "RESTAURANTE DEMO POMPEYO"
    assert receipt["audit_fields"]["provider"] == "RESTAURANTE DEMO POMPEYO"
    assert receipt["folio"] == "DEMO-81353"
    assert receipt["date"] == "2026-07-20"
    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["expense_category"]["id"] == "SIM-ALIMENTACION-81353"
    assert receipt["source"]["source_content_sha256"] == (
        "a62e64a6ef9093fc5152010da6d5f60d4dbb782442f8302023596e56fc3dfd13"
    )


def test_v6_sanitized_receipt_date_is_canonical_iso_without_contract_regression():
    backend = _backend()
    file_uuid = "14c1f293-9135-4a92-b127-cf62cb664744"
    file_record = _file(
        uuid=file_uuid,
        name="synthetic_receipt_demo_81353.png",
        mime="image/png",
    )
    parsed = {
        "receipts": [
            {
                "receipt_discriminator": "demo-81353-restaurante-demo-pompeyo-20-07-2026-clp-18-750",
                "page_metadata": {
                    "page_index": 1,
                    "page_range": [1, 1],
                    "group_label": "boleta 1",
                },
                "tipo_documento": "boleta",
                "description": "Boleta electronica ficticia con detalle de items y total en CLP.",
                "ocr_text": (
                    "DOCUMENTO FICTICIO\n"
                    "SOLO PRUEBAS - NO VALIDO TRIBUTARIAMENTE\n"
                    "RESTAURANTE DEMO POMPEYO\n"
                    "RUT: 00.000.000-0\n"
                    "BOLETA ELECTRONICA\n"
                    "FOLIO: DEMO-81353\n"
                    "FECHA: 20-07-2026\n"
                    "1 MENU EJECUTIVO $14.500\n"
                    "1 BEBIDA $ 4.250\n"
                    "TOTAL CLP $18.750\n"
                    "CATEGORIA SUGERIDA: ALIMENTACION\n"
                    "GRACIAS - DATOS 100% SINTETICOS"
                ),
                "campos_extraidos": {
                    "proveedor_razon_social": {"valor": "RESTAURANTE DEMO POMPEYO", "confianza": 100},
                    "folio": {"valor": "DEMO-81353", "confianza": 100},
                    "fecha": {"valor": "20-07-2026", "confianza": 100},
                    "moneda": {"valor": "CLP", "confianza": 100},
                    "monto_total": {"valor": "18.750", "confianza": 100},
                    "categoria": {
                        "valor": {
                            "id": "SIM-ALIMENTACION-81353",
                            "name": "ALIMENTACION",
                            "confidence": 0.95,
                            "candidates": [],
                            "ambiguous": False,
                        },
                        "confianza": 95,
                    },
                },
                "extraction_confidence": 100,
            }
        ],
        "extraction_confidence": 100,
    }
    catalog = backend._parse_category_catalog_snapshot({
        "version": "simulation-81353",
        "source": "synthetic_simulation_only",
        "categories": [
            {
                "id": "SIM-ALIMENTACION-81353",
                "name": "ALIMENTACION",
                "keywords": ["RESTAURANTE", "ALIMENTACION", "DEMO POMPEYO"],
            }
        ],
    })

    artifact = backend._build_receipt_batch_artifact(
        {file_uuid: json.dumps(parsed, ensure_ascii=False)},
        [file_record],
        [],
        [],
        [],
        catalog,
    )

    receipt = artifact["receipts"][0]
    assert artifact["batch"]["processed_count"] == 1
    assert receipt["parse_status"] == "parsed"
    assert receipt["provider"] == "RESTAURANTE DEMO POMPEYO"
    assert receipt["proveedor"] == "RESTAURANTE DEMO POMPEYO"
    assert receipt["folio"] == "DEMO-81353"
    assert receipt["date"] == "2026-07-20"
    assert receipt["audit_fields"]["date"] == "2026-07-20"
    assert next(field for field in receipt["fields"] if field["field"] == "fecha")["value"] == "2026-07-20"
    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["expense_category"]["id"] == "SIM-ALIMENTACION-81353"
    assert receipt["source"]["file_uuid"] == file_uuid
    assert receipt["source"]["receipt_discriminator"] == (
        "demo-81353-restaurante-demo-pompeyo-20-07-2026-clp-18-750"
    )
    assert "FECHA: 20-07-2026" in receipt["ocr"]["raw_preview"]


def test_receipt_date_normalization_preserves_iso_and_rejects_ambiguous_or_malformed_values():
    backend = _backend()

    assert backend._canonical_receipt_date_value("2026-07-20") == "2026-07-20"
    assert backend._canonical_receipt_date_value("20/07/2026") == "2026-07-20"
    assert backend._canonical_receipt_date_value("20-07-2026") == "2026-07-20"
    assert backend._canonical_receipt_date_value("07/08/2026") == "07/08/2026"
    assert backend._canonical_receipt_date_value("20/07") == "20/07"
    assert backend._canonical_receipt_date_value("32/07/2026") == "32/07/2026"
    assert backend._canonical_receipt_date_value("2026/07/20") == "2026/07/20"
    assert backend._canonical_receipt_date_value(None) is None


def test_acceptance_rerun_v2_same_physical_image_emits_one_auditable_receipt():
    backend = _backend()
    fixture = json.loads(
        (ROOT / "test_files/acceptance_rerun_v2_duplicate_structured_outputs.json").read_text(
            encoding="utf-8"
        )
    )
    physical_image = fixture["physical_image"]
    results = {}
    files = []
    for result in fixture["results"]:
        file_uuid = result["file_uuid"]
        results[file_uuid] = json.dumps(result["structured_output"], ensure_ascii=False)
        files.append(
            _file(
                uuid=file_uuid,
                name=physical_image["file_name"],
                mime=physical_image["mime_type"],
            )
        )
        files[-1]["content_bytes"] = physical_image["content"].encode("utf-8")
    catalog = backend._parse_category_catalog_snapshot(fixture["category_catalog_snapshot"])

    artifact = backend._build_receipt_batch_artifact(
        results,
        [],
        files,
        [],
        [],
        catalog,
    )

    assert artifact["batch"]["processed_count"] == 2
    assert len(artifact["receipts"]) == 1
    receipt = artifact["receipts"][0]
    assert receipt["receipt_id"] == "receipt_af75a07f0dc4368bd032d889"
    assert receipt["parse_status"] == "parsed"
    assert receipt["parse_method"] == "structured_json"
    assert receipt["provider"] == "RESTAURANTE DEMO POMPEYO"
    assert receipt["folio"] == "DEMO-81353"
    assert receipt["date"] == "2026-07-20"
    assert receipt["rut"] == "00.000.000-0"
    assert receipt["currency"] == "CLP"
    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["expense_category"]["id"] == "SIM-ALIMENTACION-81353"
    assert set({
        "provider": "RESTAURANTE DEMO POMPEYO",
        "folio": "DEMO-81353",
        "date": "2026-07-20",
        "rut": "00.000.000-0",
        "currency": "CLP",
    }.items()) <= set(receipt["audit_fields"].items())


def test_same_physical_image_with_weaker_runtime_duplicate_keeps_actionable_receipt():
    backend = _backend()
    content_bytes = b"same physical synthetic receipt bytes"
    full_receipt = {
        "receipts": [
            {
                "receipt_discriminator": "restaurante demo folio DEMO-81353 full",
                "page_metadata": {"page_range": [1, 1], "group_label": "boleta 1"},
                "campos_extraidos": {
                    "proveedor": {"valor": "RESTAURANTE DEMO POMPEYO", "confianza": 99, "pagina": 1},
                    "folio": {"valor": "DEMO-81353", "confianza": 99, "pagina": 1},
                    "fecha": {"valor": "2026-07-20", "confianza": 99, "pagina": 1},
                    "monto_total": {"valor": "$18.750", "confianza": 99, "pagina": 1},
                    "categoria": {
                        "valor": {
                            "id": "SIM-ALIMENTACION-81353",
                            "name": "ALIMENTACIÓN",
                            "confidence": 0.99,
                        },
                        "confianza": 99,
                        "pagina": 1,
                    },
                },
                "extraction_confidence": 99,
            }
        ]
    }
    weaker_duplicate = {
        "receipts": [
            {
                "receipt_discriminator": "restaurante demo folio DEMO-81353 weak duplicate",
                "page_metadata": {"page_range": [1, 1], "group_label": "boleta 1"},
                "campos_extraidos": {
                    "proveedor": {"valor": "RESTAURANTE DEMO POMPEYO", "confianza": 70, "pagina": 1},
                    "folio": {"valor": "DEMO-81353", "confianza": 70, "pagina": 1},
                    "fecha": {"valor": "2026-07-20", "confianza": 70, "pagina": 1},
                },
                "extraction_confidence": 70,
            }
        ]
    }
    catalog = backend._parse_category_catalog_snapshot({
        "version": "simulation-81353",
        "source": "synthetic_simulation_only",
        "categories": [
            {"id": "SIM-ALIMENTACION-81353", "name": "ALIMENTACIÓN", "keywords": ["ALIMENTACIÓN"]},
        ],
    })

    artifact = backend._build_receipt_batch_artifact(
        {
            "runtime-weak": json.dumps(weaker_duplicate, ensure_ascii=False),
            "runtime-full": json.dumps(full_receipt, ensure_ascii=False),
        },
        [],
        [
            {
                **_file(uuid="runtime-weak", name="synthetic_receipt_demo_81353.png", mime="image/png"),
                "content_bytes": content_bytes,
            },
            {
                **_file(uuid="runtime-full", name="synthetic_receipt_demo_81353.png", mime="image/png"),
                "content_bytes": content_bytes,
            },
        ],
        [],
        [],
        catalog,
    )

    assert artifact["batch"]["processed_count"] == 2
    assert len(artifact["receipts"]) == 1
    receipt = artifact["receipts"][0]
    assert receipt["source"]["file_uuid"] == "runtime-full"
    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["expense_category"]["id"] == "SIM-ALIMENTACION-81353"
    assert receipt["provider"] == "RESTAURANTE DEMO POMPEYO"
    assert receipt["folio"] == "DEMO-81353"
    assert receipt["date"] == "2026-07-20"


def test_plain_text_total_fallback_requires_total_label_and_currency_context():
    backend = _backend()
    raw_text = """
    RESTAURANTE DEMO POMPEYO
    RUT: 76.123.456-7
    FOLIO: 18750
    FECHA: 20/07/2026
    TOTAL CLP $18.750
    CATEGORIA: ALIMENTACION
    """

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": raw_text},
        [_file()],
        [],
        [],
        [],
    )

    receipt = artifact["receipts"][0]
    assert receipt["parse_method"] == "deterministic_text_fallback"
    assert receipt["proposed_amount"]["numeric_value"] == 18750


def test_chilean_total_formats_normalize_to_same_integer_amount():
    backend = _backend()

    assert backend._parse_numeric_value("TOTAL CLP $18.750") == 18750
    assert backend._parse_numeric_value("$18.750") == 18750
    assert backend._parse_numeric_value("CLP 18.750,00") == 18750
    assert backend._parse_numeric_value("$18,750") == 18750
    assert backend._parse_numeric_value("18750") == 18750


def test_explicit_malformed_amount_does_not_invent_candidate_value():
    backend = _backend()
    parsed = {"monto": {"valor": "valor ilegible", "confianza": 60}}

    receipt = backend._receipt_entry(_file(), parsed, parsed, json.dumps(parsed))

    assert receipt["proposed_amount"]["numeric_value"] is None
    assert receipt["proposed_amount"]["selection_rule"] == "no_numeric_amount_candidate"


def test_non_amount_identity_numbers_are_not_treated_as_amounts():
    backend = _backend()
    parsed = {
        "campos_extraidos": {
            "folio": {"valor": "18750", "confianza": 99},
            "rut_proveedor": {"valor": "76.123.456-7", "confianza": 99},
            "fecha_emision": {"valor": "20/07/2026", "confianza": 99},
        }
    }

    receipt = backend._receipt_entry(_file(), parsed, parsed, json.dumps(parsed))

    assert receipt["amount_candidates"] == []
    assert receipt["proposed_amount"]["selection_rule"] == "no_numeric_amount_candidate"


def test_text_fallback_does_not_treat_rut_date_or_folio_as_amounts():
    backend = _backend()
    raw_text = """
    RESTAURANTE DEMO POMPEYO
    RUT: 76.123.456-7
    FOLIO: 18750
    FECHA: 20/07/2026
    """

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": raw_text},
        [_file()],
        [],
        [],
        [],
    )

    receipt = artifact["receipts"][0]
    assert receipt["parse_status"] == "parsed"
    assert receipt["amount_candidates"] == []
    assert receipt["proposed_amount"]["numeric_value"] is None
    assert receipt["proposed_amount"]["selection_rule"] == "no_numeric_amount_candidate"


def test_text_fallback_malformed_no_receipt_stays_not_detected():
    backend = _backend()
    raw_text = "imagen borrosa sin texto legible ni campos de boleta"

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": raw_text},
        [_file()],
        [],
        [],
        [],
    )

    receipt = artifact["receipts"][0]
    assert receipt["receipt_id"] is None
    assert receipt["parse_status"] == "not_detected"
    assert receipt["parse_error"] == "malformed_or_non_json_extraction"


def test_multiple_total_candidates_with_close_confidence_stay_ambiguous():
    backend = _backend()
    parsed = {
        "amount_candidates": [
            {"label": "TOTAL", "value": "$18.750", "confidence": 0.95},
            {"label": "TOTAL", "value": "$19.750", "confidence": 0.94},
        ]
    }

    receipt = backend._receipt_entry(_file(), parsed, parsed, json.dumps(parsed))

    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["proposed_amount"]["ambiguous"] is True


def test_text_fallback_multiple_total_candidates_stay_ambiguous():
    backend = _backend()
    raw_text = """
    RESTAURANTE DEMO POMPEYO
    TOTAL CLP $18.750
    TOTAL CLP $19.750
    """

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": raw_text},
        [_file()],
        [],
        [],
        [],
    )

    receipt = artifact["receipts"][0]
    assert receipt["proposed_amount"]["numeric_value"] == 18750
    assert receipt["proposed_amount"]["ambiguous"] is True


def test_structured_json_path_keeps_parse_method_and_existing_amount_shape():
    backend = _backend()
    parsed = {
        "campos_extraidos": {
            "monto_total": {"valor": "$18.750", "confianza": 99},
        },
        "extraction_confidence": 99,
    }

    receipt = backend._receipt_entry(_file(), parsed, parsed, json.dumps(parsed))

    assert receipt["parse_method"] == "structured_json"
    assert receipt["parse_status"] == "parsed"
    assert receipt["proposed_amount"]["numeric_value"] == 18750


def test_two_distinct_receipts_in_one_pdf_emit_two_artifact_receipts():
    backend = _backend()
    parsed = {
        "receipts": [
            {
                "receipt_discriminator": "folio A proveedor Uno total 10000",
                "page_metadata": {"page_index": 1, "page_range": [1, 1], "group_label": "boleta-a"},
                "campos_extraidos": {
                    "folio": {"valor": "A", "confianza": 95, "pagina": 1},
                    "monto_total": {"valor": "$10.000", "confianza": 90, "pagina": 1},
                },
                "extraction_confidence": 90,
            },
            {
                "receipt_discriminator": "folio B proveedor Dos total 20000",
                "page_metadata": {"page_index": 2, "page_range": [2, 2], "group_label": "boleta-b"},
                "campos_extraidos": {
                    "folio": {"valor": "B", "confianza": 94, "pagina": 2},
                    "monto_total": {"valor": "$20.000", "confianza": 91, "pagina": 2},
                },
                "extraction_confidence": 88,
            },
        ]
    }

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": json.dumps(parsed)},
        [_file(name="multi.pdf")],
        [],
        [],
        [],
    )

    assert len(artifact["attachment_inventory"]) == 1
    assert len(artifact["receipts"]) == 2
    assert artifact["receipts"][0]["receipt_id"] != artifact["receipts"][1]["receipt_id"]
    assert artifact["receipts"][0]["source"]["file_uuid"] == "file-1"
    assert artifact["receipts"][1]["proposed_amount"]["numeric_value"] == 20000


def test_no_folio_same_page_receipts_with_distinct_discriminators_remain_two():
    backend = _backend()
    parsed = {
        "receipts": [
            {
                "receipt_discriminator": "mesa 12 venta tarjeta",
                "page_metadata": {"page_index": 1, "page_range": [1, 1]},
                "campos_extraidos": {
                    "proveedor": {"valor": "RESTAURANTE DEMO", "confianza": 95, "pagina": 1},
                    "fecha": {"valor": "2026-07-20", "confianza": 95, "pagina": 1},
                    "monto_total": {"valor": "$10.000", "confianza": 90, "pagina": 1},
                },
                "categoria_propuesta": {"id": "ALIMENTACION", "name": "Alimentacion"},
                "extraction_confidence": 90,
            },
            {
                "receipt_discriminator": "mesa 14 venta efectivo",
                "page_metadata": {"page_index": 1, "page_range": [1, 1]},
                "campos_extraidos": {
                    "proveedor": {"valor": "RESTAURANTE DEMO", "confianza": 95, "pagina": 1},
                    "fecha": {"valor": "2026-07-20", "confianza": 95, "pagina": 1},
                    "monto_total": {"valor": "$10.000", "confianza": 90, "pagina": 1},
                },
                "categoria_propuesta": {"id": "ALIMENTACION", "name": "Alimentacion"},
                "extraction_confidence": 90,
            },
        ]
    }

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": json.dumps(parsed)},
        [_file(name="same-page-no-folio.pdf")],
        [],
        [],
        [],
    )

    assert len(artifact["receipts"]) == 2
    assert artifact["receipts"][0]["receipt_id"] != artifact["receipts"][1]["receipt_id"]
    assert [
        receipt["source"]["receipt_discriminator"]
        for receipt in artifact["receipts"]
    ] == ["mesa-12-venta-tarjeta", "mesa-14-venta-efectivo"]


def test_one_receipt_can_span_two_pdf_pages():
    backend = _backend()
    parsed = {
        "receipts": [
            {
                "receipt_discriminator": "folio C spanning pages",
                "page_metadata": {"page_index": 1, "page_range": [1, 2], "group_label": "boleta-c"},
                "campos_extraidos": {
                    "folio": {"valor": "C", "confianza": 95, "pagina": 1},
                    "monto_total": {"valor": "$30.000", "confianza": 89, "pagina": 2},
                },
            }
        ]
    }

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": json.dumps(parsed)},
        [_file(name="spanning.pdf")],
        [],
        [],
        [],
    )

    assert len(artifact["receipts"]) == 1
    receipt = artifact["receipts"][0]
    assert receipt["source"]["page_metadata"]["page_range"] == [1, 2]
    assert receipt["source"]["page_metadata"]["group_label"] == "boleta-c"


def test_receipt_ids_are_stable_across_replay():
    parsed = {
        "receipts": [
            {
                "receipt_discriminator": "Folio 123 Proveedor ACME Total 10000",
                "page_metadata": {"page_range": [1, 1]},
                "campos_extraidos": {
                    "folio": {"valor": "123", "confianza": 99, "pagina": 1},
                    "monto_total": {"valor": "$10.000", "confianza": 95, "pagina": 1},
                },
            }
        ]
    }

    backend_a = _backend()
    backend_b = _backend()
    artifact_a = backend_a._build_receipt_batch_artifact(
        {"file-1": json.dumps(parsed)}, [_file()], [], [], []
    )
    artifact_b = backend_b._build_receipt_batch_artifact(
        {"file-1": json.dumps(parsed)}, [_file()], [], [], []
    )

    assert artifact_a["receipts"][0]["receipt_id"] == artifact_b["receipts"][0]["receipt_id"]


def test_no_detected_receipts_from_valid_json_is_not_detected():
    backend = _backend()
    parsed = {"receipts": [], "observaciones": "no hay boletas"}

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": json.dumps(parsed)}, [_file()], [], [], []
    )

    assert len(artifact["receipts"]) == 1
    assert artifact["receipts"][0]["receipt_id"] is None
    assert artifact["receipts"][0]["parse_status"] == "not_detected"
    assert artifact["receipts"][0]["parse_error"] == "no_receipts_detected"


def test_artifact_redacts_secret_like_values():
    backend = _backend()
    parsed = {
        "campos_extraidos": {
            "observacion": {"valor": "sk-test_secret_value_1234567890", "confianza": 99},
        },
        "extraction_confidence": 99,
    }

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": json.dumps(parsed)},
        [_file()],
        [],
        [],
        [],
    )

    payload = json.dumps(artifact)
    assert "sk-test_secret_value_1234567890" not in payload
    assert "[REDACTED_SECRET]" in payload


def test_artifact_contains_source_content_sha256_and_page_metadata():
    backend = _backend()
    parsed = {
        "campos_extraidos": {
            "monto_total": {"valor": "$10.000", "confianza": 90, "pagina": 2},
        }
    }

    artifact = backend._build_receipt_batch_artifact(
        {"file-1": json.dumps(parsed)},
        [_file()],
        [],
        [],
        [],
    )

    source = artifact["receipts"][0]["source"]
    assert source["source_content_sha256"]
    assert source["page_metadata"]
    candidate_source = artifact["receipts"][0]["amount_candidates"][0]["source"]
    assert candidate_source["source_content_sha256"] == source["source_content_sha256"]
    assert candidate_source["page_metadata"]["page_range"] == [2, 2]


def test_upload_returns_ready_receipts_uuid_and_payload_metadata(monkeypatch):
    backend = _backend()
    uploaded = {}

    class UploadManager:
        def call(self, name, **kwargs):
            assert name == "upload_file"
            assert "orchestration_session_uuid" not in kwargs
            assert kwargs["orchestration_session_uuids"] == ["session-uuid"]
            assert kwargs["internal_orchestration_session_uuid"] == "internal-session-uuid"
            uploaded["filename"] = kwargs["file"].name
            uploaded["content"] = kwargs["file"].getvalue()
            return {"file_uuid": "ready-uuid"}

    monkeypatch.setattr(function_logic, "files_api_manager", UploadManager())
    artifact = backend._build_receipt_batch_artifact({}, [], [], [], [])

    ready_uuid = backend._upload_receipt_batch_artifact(artifact)

    assert ready_uuid == "ready-uuid"
    assert uploaded["filename"] == "pompeyo_receipt_batch.json"
    decoded = json.loads(uploaded["content"].decode("utf-8"))
    assert decoded["schema_version"] == ARTIFACT_SCHEMA_VERSION


def test_final_response_returns_uuid_not_inline_artifact(monkeypatch):
    backend = _backend()

    class UploadManager:
        def call(self, name, **kwargs):
            assert name == "upload_file"
            assert kwargs["orchestration_session_uuids"] == ["session-uuid"]
            return {"file_uuid": "ready-uuid"}

    monkeypatch.setattr(function_logic, "files_api_manager", UploadManager())

    response = backend._build_final_response({}, [], [], [], [], "sin adjuntos")
    payload = json.loads(response.split("\n\n", 1)[0])

    assert payload["ready_receipts_uuid"] == "ready-uuid"
    assert payload["schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert "receipts" not in payload
