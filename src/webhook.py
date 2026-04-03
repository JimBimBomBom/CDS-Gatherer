"""
webhook.py
----------
Notifies cds-app that a new SQL file is ready so it can reload
MySQL + Elasticsearch via its /internal/reload endpoint.

The notification is a simple HTTP POST.  The SQL file itself is at a
well-known path inside the shared volume; the app knows where to find it.

Environment variables
~~~~~~~~~~~~~~~~~~~~~
CDS_APP_BASE_URL    Base URL of cds-app (default: http://cds-app:8080)
RELOAD_ENDPOINT     Path for the reload webhook (default: /internal/reload)
WEBHOOK_TIMEOUT     Seconds to wait for a response (default: 30)
"""

from __future__ import annotations

import logging
import os

import httpx

logger = logging.getLogger(__name__)

CDS_APP_BASE_URL = os.environ.get("CDS_APP_BASE_URL", "http://cds-app:8080")
RELOAD_ENDPOINT = os.environ.get("RELOAD_ENDPOINT", "/internal/reload")
WEBHOOK_TIMEOUT = int(os.environ.get("WEBHOOK_TIMEOUT", "30"))

RELOAD_URL = f"{CDS_APP_BASE_URL}{RELOAD_ENDPOINT}"


def notify_reload(sql_file_path: str, enabled: bool = True) -> bool:
    """
    POST to cds-app asking it to reload from *sql_file_path*.

    When *enabled* is ``False`` (e.g. ``--no-webhook`` CLI flag) the call is
    skipped entirely and ``True`` is returned so callers treat it as a success.

    Returns True on success (2xx) or when disabled, False on any error.
    The caller decides whether a failure is fatal.
    """
    if not enabled:
        logger.info("Webhook notifications disabled; skipping reload POST.")
        return True

    payload = {"sqlFilePath": sql_file_path}
    logger.info("Sending reload notification to %s (file: %s)", RELOAD_URL, sql_file_path)

    try:
        with httpx.Client(timeout=WEBHOOK_TIMEOUT) as client:
            response = client.post(RELOAD_URL, json=payload)

        if response.is_success:
            logger.info(
                "Reload notification accepted. HTTP %d: %s",
                response.status_code,
                response.text[:200],
            )
            return True

        logger.error(
            "Reload notification rejected. HTTP %d: %s",
            response.status_code,
            response.text[:500],
        )
        return False

    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send reload notification: %s", exc)
        return False
