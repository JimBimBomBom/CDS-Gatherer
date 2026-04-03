"""
wikidata_service.py
-------------------
Fetches city data from the Wikidata SPARQL endpoint for a given language.

Mirrors the behaviour of the C# WikidataService:
  - Paginated requests (PAGE_SIZE rows per request, up to MAX_PAGES pages)
  - ORDER BY ?city for stable, repeatable pagination
  - Deduplication guard (OFFSET-boundary shifts on live data can overlap)
  - Polite 10-second delay between pages
  - Strips raw control characters that System.Text.Json (and Python json) reject
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
PAGE_SIZE = 20_000
MAX_PAGES = 40
PAGE_DELAY_SECONDS = 10
HTTP_TIMEOUT_SECONDS = 180  # 3 minutes, matching the C# service

# Retry config for transient page failures (5xx, 429, network errors)
MAX_PAGE_RETRIES = max(0, int(os.environ.get("MAX_PAGE_RETRIES", "3")))
RETRY_BASE_DELAY_SECONDS = max(1, int(os.environ.get("RETRY_BASE_DELAY_SECONDS", "30")))

# HTTP status codes that are worth retrying (transient server-side issues)
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_HEADERS = {
    "User-Agent": "CityDistanceService/1.0 (https://yourdomain.example; your-email@example.com)",
    "Accept": "application/sparql-results+json",
}


@dataclass
class SparqlCityInfo:
    wikidata_id: str
    city_name: str
    language: str
    latitude: float
    longitude: float
    country: Optional[str] = None
    country_code: Optional[str] = None
    admin_region: Optional[str] = None
    population: Optional[int] = None


def _build_query(language: str, limit: int, offset: int) -> str:
    """
    Build the SPARQL query for a given language, page size and offset.
    ORDER BY ?city is essential for stable pagination.
    """
    return f"""
SELECT ?city ?label ?lat ?lon ?countryLabel ?iso2 ?adminLabel ?pop WHERE {{
    ?city wdt:P625 ?coord .
    ?city wdt:P31/wdt:P279* wd:Q515 .

    OPTIONAL {{
        ?city wdt:P17 ?country .
        OPTIONAL {{ ?country wdt:P297 ?iso2 . }}
    }}

    OPTIONAL {{
        ?city wdt:P131 ?admin .
    }}

    OPTIONAL {{ ?city wdt:P1082 ?pop . }}

    BIND(geof:latitude(?coord)  AS ?lat)
    BIND(geof:longitude(?coord) AS ?lon)

    SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "{language},en" .
        ?city rdfs:label ?label .
        ?country rdfs:label ?countryLabel .
        ?admin rdfs:label ?adminLabel .
    }}
}}
ORDER BY ?city
LIMIT {limit}
OFFSET {offset}"""


def _sanitise_raw(raw: str) -> str:
    """
    Strip control characters that JSON parsers reject.
    Mirrors the C# Regex.Replace(raw, @"[\x00-\x08\x0B\x0C\x0E-\x1F]", " ") logic.
    Keeps \t (0x09) and \n (0x0A); normalises CR/CRLF.
    """
    # Remove non-printable control chars except \t and \n
    raw = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", " ", raw)
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    return raw


def _parse_response(raw: str, language: str) -> list[SparqlCityInfo]:
    """Parse the raw SPARQL JSON response into a list of SparqlCityInfo objects."""
    data = json.loads(_sanitise_raw(raw))
    bindings = data["results"]["bindings"]
    cities: list[SparqlCityInfo] = []

    for row in bindings:
        try:
            if not all(k in row for k in ("city", "label", "lat", "lon")):
                continue

            wikidata_id = row["city"]["value"].rsplit("/", 1)[-1]
            city_name = row["label"]["value"]

            try:
                lat = float(row["lat"]["value"])
                lon = float(row["lon"]["value"])
            except (ValueError, KeyError):
                continue

            country = row.get("countryLabel", {}).get("value")
            country_code = row.get("iso2", {}).get("value")
            admin_region = row.get("adminLabel", {}).get("value")

            population: Optional[int] = None
            pop_raw = row.get("pop", {}).get("value")
            if pop_raw:
                try:
                    population = int(pop_raw)
                except ValueError:
                    pass

            cities.append(
                SparqlCityInfo(
                    wikidata_id=wikidata_id,
                    city_name=city_name,
                    language=language,
                    latitude=lat,
                    longitude=lon,
                    country=country,
                    country_code=country_code,
                    admin_region=admin_region,
                    population=population,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] Skipping row due to parse error: %s", language, exc)

    return cities


def _fetch_page_with_retry(
    client: httpx.Client,
    language: str,
    page_number: int,
    offset: int,
) -> list[SparqlCityInfo] | None:
    """
    Fetch a single SPARQL page, retrying up to MAX_PAGE_RETRIES times on
    transient failures (5xx, 429, network errors) with exponential backoff.

    Returns the parsed list of cities on success, or ``None`` if every attempt
    fails (signals the caller to stop pagination).
    """
    query = _build_query(language, PAGE_SIZE, offset)
    delay = RETRY_BASE_DELAY_SECONDS

    for attempt in range(1, MAX_PAGE_RETRIES + 2):  # attempt 1 = first try
        try:
            response = client.post(SPARQL_ENDPOINT, data={"query": query})
            raw = response.text

            logger.info(
                "[%s] Page %d – HTTP %d %s (attempt %d)",
                language, page_number, response.status_code, response.reason_phrase, attempt,
            )

            # 429: honour Retry-After if the server sends one
            if response.status_code == 429:
                retry_after = _parse_retry_after(response, fallback=delay)
                logger.warning(
                    "[%s] Page %d – rate limited (429). Waiting %ds before retry...",
                    language, page_number, retry_after,
                )
                time.sleep(retry_after)
                delay = min(delay * 2, 300)
                continue

            if response.status_code in _RETRYABLE_STATUS_CODES:
                snippet = raw[: min(200, len(raw))]
                raise RuntimeError(f"HTTP {response.status_code}: {snippet}")

            if not response.is_success:
                # Non-retryable client error (4xx other than 429) — fail immediately
                snippet = raw[: min(500, len(raw))]
                logger.error(
                    "[%s] Page %d – non-retryable error HTTP %d. Body: %s",
                    language, page_number, response.status_code, snippet,
                )
                return None

            return _parse_response(raw, language)

        except Exception as exc:  # noqa: BLE001
            is_last_attempt = attempt == MAX_PAGE_RETRIES + 1
            if is_last_attempt:
                logger.error(
                    "[%s] Page %d – attempt %d/%d failed: %s. No more retries.",
                    language, page_number, attempt, MAX_PAGE_RETRIES + 1, exc,
                )
                return None

            logger.warning(
                "[%s] Page %d – attempt %d/%d failed: %s. Retrying in %ds...",
                language, page_number, attempt, MAX_PAGE_RETRIES + 1, exc, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 300)  # cap at 5 minutes

    return None  # unreachable, but satisfies the type checker


def _parse_retry_after(response: httpx.Response, fallback: int) -> int:
    """
    Parse the ``Retry-After`` response header (integer seconds or HTTP-date).
    Returns *fallback* if the header is absent or unparseable.
    """
    header = response.headers.get("retry-after", "").strip()
    if not header:
        return fallback
    try:
        return max(1, int(header))
    except ValueError:
        pass
    # HTTP-date format: "Fri, 03 Apr 2026 15:00:00 GMT"
    try:
        from email.utils import parsedate_to_datetime
        retry_at = parsedate_to_datetime(header)
        wait = int((retry_at - retry_at.utcnow()).total_seconds())
        return max(1, wait)
    except Exception:  # noqa: BLE001
        return fallback


def fetch_cities(language: str) -> list[SparqlCityInfo]:
    """
    Fetch all cities for *language* from the Wikidata SPARQL endpoint.

    Returns a deduplicated list of SparqlCityInfo objects.

    Each page is attempted up to MAX_PAGE_RETRIES + 1 times with exponential
    backoff before being considered failed.  Partial results (cities collected
    before the failing page) are always returned rather than discarded.
    """
    all_cities: list[SparqlCityInfo] = []
    seen_ids: set[str] = set()
    offset = 0

    logger.info("[%s] Starting paginated fetch (page size: %d)", language, PAGE_SIZE)

    with httpx.Client(headers=_HEADERS, timeout=HTTP_TIMEOUT_SECONDS) as client:
        for page_number in range(1, MAX_PAGES + 1):
            logger.info("[%s] Fetching page %d (offset %d)...", language, page_number, offset)

            page = _fetch_page_with_retry(client, language, page_number, offset)
            if page is None:
                # All retry attempts exhausted — keep what we have and stop.
                logger.error(
                    "[%s] Page %d failed after all retries. "
                    "Stopping pagination, keeping %d cities collected so far.",
                    language, page_number, len(all_cities),
                )
                break

            # Deduplicate (live edits can shift boundary rows between pages)
            new_count = 0
            for city in page:
                if city.wikidata_id not in seen_ids:
                    seen_ids.add(city.wikidata_id)
                    all_cities.append(city)
                    new_count += 1

            duplicates = len(page) - new_count
            logger.info(
                "[%s] Page %d: %d rows returned, %d new, %d duplicates skipped. Running total: %d",
                language, page_number, len(page), new_count, duplicates, len(all_cities),
            )

            if len(page) < PAGE_SIZE:
                logger.info(
                    "[%s] Last page reached (got %d < %d). Done.",
                    language, len(page), PAGE_SIZE,
                )
                break

            offset += PAGE_SIZE

            if page_number < MAX_PAGES:
                logger.info("[%s] Waiting %ds before next page...", language, PAGE_DELAY_SECONDS)
                time.sleep(PAGE_DELAY_SECONDS)
        else:
            logger.warning(
                "[%s] WARNING: hit MAX_PAGES (%d) safety cap. There may be more cities.",
                language, MAX_PAGES,
            )

    logger.info("[%s] Fetch complete. Total unique cities: %d", language, len(all_cities))
    return all_cities
