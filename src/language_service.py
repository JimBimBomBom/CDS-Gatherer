"""
language_service.py
-------------------
Fetches the list of supported languages from the cds-app /languages endpoint
and returns the unique Wikidata-compatible language tag list.

The /languages endpoint returns objects like:
    {"code": "en", "name": "English", ...}
    {"code": "en-US", "name": "English (US)", ...}

Wikidata language tags use BCP-47 style codes but only the base subtag matters
for the SERVICE wikibase:label query (e.g. "en-US" → "en").  We deduplicate
so we don't fire the same SPARQL query twice.

If the cds-app is not reachable (e.g. during a first-boot race), a configurable
fallback list is used instead.
"""

from __future__ import annotations

import logging
import os

import httpx

logger = logging.getLogger(__name__)

# Env-configurable base URL of the cds-app service (docker-compose service name)
CDS_APP_BASE_URL = os.environ.get("CDS_APP_BASE_URL", "http://cds-app:8080")
LANGUAGES_ENDPOINT = f"{CDS_APP_BASE_URL}/languages"
FETCH_TIMEOUT_SECONDS = 10

# Fallback used when the app is not reachable
_FALLBACK_LANGUAGES = [
    "en", "cs", "sk", "de", "fr", "es", "it", "pt",
    "pl", "nl", "ru", "ja", "zh", "ar", "ko", "sv",
    "tr", "fi", "hu", "no",
]


def _normalise_code(code: str) -> str:
    """
    Strip region subtag: 'en-US' → 'en', 'zh-Hant' → 'zh'.
    Wikidata SERVICE wikibase:label accepts the base tag.
    """
    return code.split("-")[0].lower()


def fetch_language_codes(override: str | None = None) -> list[str]:
    """
    Return a deduplicated, ordered list of Wikidata-compatible language codes.

    If *override* is provided (a comma-separated string such as ``"en,de,fr"``),
    it is parsed and returned immediately without contacting cds-app at all.

    Otherwise, the list is fetched from the cds-app /languages endpoint.
    Order matches the /languages response; first occurrence of each base code wins.
    Falls back to _FALLBACK_LANGUAGES if the endpoint is unreachable.
    """
    if override is not None:
        codes = [_normalise_code(c.strip()) for c in override.split(",") if c.strip()]
        # deduplicate while preserving order
        seen: set[str] = set()
        unique = [c for c in codes if not (c in seen or seen.add(c))]  # type: ignore[func-returns-value]
        logger.info("Using language override list (%d): %s", len(unique), unique)
        return unique

    try:
        with httpx.Client(timeout=FETCH_TIMEOUT_SECONDS) as client:
            response = client.get(LANGUAGES_ENDPOINT)
            response.raise_for_status()
            data = response.json()

        codes: list[str] = []
        seen: set[str] = set()
        for entry in data:
            raw_code = entry.get("code", "")
            base = _normalise_code(raw_code)
            if base and base not in seen:
                seen.add(base)
                codes.append(base)

        if codes:
            logger.info("Fetched %d unique language codes from %s: %s", len(codes), LANGUAGES_ENDPOINT, codes)
            return codes

        logger.warning("Language endpoint returned an empty list; using fallback.")

    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Could not fetch languages from %s (%s). Using fallback list.",
            LANGUAGES_ENDPOINT, exc,
        )

    logger.info("Using fallback language list: %s", _FALLBACK_LANGUAGES)
    return list(_FALLBACK_LANGUAGES)
