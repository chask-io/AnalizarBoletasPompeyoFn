import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

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

from backend.function_logic import ARTIFACT_SCHEMA_VERSION, FunctionBackend


def _backend():
    backend = FunctionBackend.__new__(FunctionBackend)
    backend.orchestration_event = SimpleNamespace(
        orchestration_session_uuid="session-uuid",
        internal_orchestration_session_uuid="internal-session-uuid",
        event_id="event-uuid",
    )
    return backend


def _file(uuid="file-1", name="boleta.pdf", mime="application/pdf"):
    return {
        "file_uuid": uuid,
        "file_name": name,
        "mime_type": mime,
        "size": 1234,
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


def test_missing_attachments_emit_batch_artifact_instead_of_special_case_text():
    backend = _backend()

    artifact = backend._build_receipt_batch_artifact({}, [], [], [], [("N/A", "No hay archivos en la sesion")])

    assert artifact["schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert artifact["batch"]["missing_attachments"] is True
    assert artifact["attachment_inventory"][0]["status"] == "skipped"
    assert artifact["receipts"] == []


def test_malformed_ocr_is_marked_without_throwing():
    backend = _backend()

    receipt = backend._receipt_entry(_file(), None, "not-json-response")

    assert receipt["parse_status"] == "malformed"
    assert receipt["parse_error"] == "malformed_or_non_json_extraction"
    assert receipt["ocr"]["raw_content_type"] == "text"


def test_category_ambiguity_is_explicit_and_keeps_candidate_schema():
    backend = _backend()
    parsed = {
        "description": "Compra en Copec con pago de estacionamiento",
        "campos_extraidos": {
            "proveedor": {"valor": "Copec Parking", "confianza": 95},
        },
    }

    category = backend._category_candidates(parsed, _file())

    assert category["ambiguous"] is True
    assert {"id", "name", "source", "confidence", "candidates"} <= set(category)
    assert len(category["candidates"]) >= 2


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
