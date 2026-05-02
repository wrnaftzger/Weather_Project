from __future__ import annotations

import os
import sys
import time

from dotenv import load_dotenv
from flask import Flask, jsonify

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

REQUIRED_ENV_VARS = [
    "AZURE_SQL_USER",
    "AZURE_SQL_PASSWORD",
]
STARTUP_RETRY_ATTEMPTS = int(os.getenv("APP_STARTUP_RETRY_ATTEMPTS", "4"))
STARTUP_RETRY_DELAY_SECONDS = float(os.getenv("APP_STARTUP_RETRY_DELAY_SECONDS", "2.0"))


def _missing_required_env() -> list[str]:
    return [key for key in REQUIRED_ENV_VARS if not os.getenv(key)]


def _create_fallback_app(reason: str, details: str | None = None) -> Flask:
    fallback = Flask(__name__)

    @fallback.get("/")
    def root():
        payload = {
            "status": "configuration_error",
            "reason": reason,
            "required_env": REQUIRED_ENV_VARS,
        }
        if details:
            payload["details"] = details
        return jsonify(payload), 503

    @fallback.get("/health")
    def health():
        return jsonify({"ok": False, "reason": reason}), 503

    return fallback


def _is_transient_startup_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    transient_markers = (
        " 40613",
        "(40613",
        "database 'weather'",
        "not currently available",
        "adaptive server connection failed",
        "timeout",
        "temporar",
        "connection reset",
    )
    return any(marker in msg for marker in transient_markers)


def create_app():
    missing_env = _missing_required_env()
    if missing_env:
        return _create_fallback_app(
            reason="Missing required environment variables.",
            details=f"Missing: {', '.join(missing_env)}",
        )

    last_exc: Exception | None = None
    attempts = max(1, STARTUP_RETRY_ATTEMPTS)
    for attempt in range(1, attempts + 1):
        try:
            from visualization.globe_app import build_app

            dash_app = build_app()
            return dash_app.server
        except Exception as exc:  # pragma: no cover - protects serverless cold starts
            last_exc = exc
            if not _is_transient_startup_error(exc) or attempt >= attempts:
                break
            time.sleep(STARTUP_RETRY_DELAY_SECONDS * attempt)

    assert last_exc is not None
    return _create_fallback_app(
        reason="Application failed to initialize.",
        details=f"{type(last_exc).__name__}: {last_exc}",
    )


app = create_app()
