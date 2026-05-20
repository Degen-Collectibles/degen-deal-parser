import importlib


def test_legacy_worker_service_entrypoint_imports_canonical_worker_service():
    legacy = importlib.import_module("app.worker_service")
    canonical = importlib.import_module("app.discord.worker_service")

    assert legacy.run_worker_service is canonical.run_worker_service
    assert legacy.main is canonical.main
