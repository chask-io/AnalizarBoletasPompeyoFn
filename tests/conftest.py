import sys
import types


class DummyManager:
    def call(self, *args, **kwargs):
        raise AssertionError("network/API calls are not allowed in local unit tests")


api_mod = types.ModuleType("api")
files_mod = types.ModuleType("api.files_requests")
pipeline_mod = types.ModuleType("api.pipeline_requests")
files_mod.files_api_manager = DummyManager()
pipeline_mod.pipeline_api_manager = DummyManager()

sys.modules.setdefault("api", api_mod)
sys.modules.setdefault("api.files_requests", files_mod)
sys.modules.setdefault("api.pipeline_requests", pipeline_mod)
