import sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from telegram.tele_main import app
from fastapi.testclient import TestClient


class _FakeDatabaseManager:
	def compare_versions(self):
		return {
			"sheets_external_last_edit_ts": None,
			"sqlite_external_last_edit_ts": None,
			"sheets_python_last_write_ts": None,
			"sqlite_python_last_write_ts": None,
			"verdict": "already_in_sync_or_no_external_changes",
		}

	def compare_and_sync(self):
		return {
			"action": "none",
			"verdict": "already_in_sync_or_no_external_changes",
			"sync_result": [],
			"sheets_external_last_edit_ts": None,
			"sqlite_external_last_edit_ts": None,
		}


def main() -> None:
    paths = {route.path for route in app.routes}
    required_paths = {
        "/",
        "/health",
        "/api/tables",
        "/sync/status",
        "/sync",
        "/telegram/webhook",
    }

    missing = sorted(required_paths - paths)
    if missing:
        raise RuntimeError(f"Missing required routes: {missing}")

    client = TestClient(app)

    with ExitStack() as stack:
        stack.enter_context(patch("telegram.tele_main.DatabaseManager", _FakeDatabaseManager))

        status_response = client.get("/sync/status")
        if status_response.status_code != 200:
            raise RuntimeError(f"/sync/status failed: {status_response.status_code} {status_response.text}")

        sync_response = client.post("/sync", params={"action": "auto"})
        if sync_response.status_code != 200:
            raise RuntimeError(f"/sync failed: {sync_response.status_code} {sync_response.text}")

        status_payload = status_response.json()
        sync_payload = sync_response.json()

        if not status_payload.get("ok") or "status" not in status_payload:
            raise RuntimeError(f"Unexpected /sync/status response: {status_payload}")

        if not sync_payload.get("ok") or sync_payload.get("action") != "none":
            raise RuntimeError(f"Unexpected /sync response: {sync_payload}")

    print("CI smoke check passed: required routes are registered and sync endpoints respond.")


if __name__ == "__main__":
    main()
