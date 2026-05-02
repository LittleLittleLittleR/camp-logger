import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from telegram.tele_main import app


def main() -> None:
    paths = {route.path for route in app.routes}
    required_paths = {
        "/",
        "/health",
        "/api/tables",
        "/api/table/{table_name}",
        "/telegram/webhook",
    }

    missing = sorted(required_paths - paths)
    if missing:
        raise RuntimeError(f"Missing required routes: {missing}")

    print("CI smoke check passed: required routes are registered.")


if __name__ == "__main__":
    main()
