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

    receipt = backend._receipt_entry(_file(), None, "not-json-response")

    assert receipt["parse_status"] == "malformed"
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

    receipt = backend._receipt_entry(_file(), parsed, json.dumps(parsed))

    assert receipt["proposed_amount"]["numeric_value"] == 10000
    assert receipt["proposed_amount"]["candidate_id"] == "file-1:monto_total"
    assert receipt["expense_category"]["status"] == "unresolved"


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
            return {"file_uuid": "ready-uuid"}

    monkeypatch.setattr(function_logic, "files_api_manager", UploadManager())

    response = backend._build_final_response({}, [], [], [], [], "sin adjuntos")
    payload = json.loads(response.split("\n\n", 1)[0])

    assert payload["ready_receipts_uuid"] == "ready-uuid"
    assert payload["schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert "receipts" not in payload
