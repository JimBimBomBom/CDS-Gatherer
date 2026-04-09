"""
wikidata_service.py
-------------------
Fetches maximum city data from Wikidata SPARQL endpoint.

Uses multi-pass approach for reliability:
  - Pass 1: Core data (id, name, lat, lon)
  - Pass 2: Country
  - Pass 3: Population
  - Pass 4: Country code
  - Pass 5: Admin region

All cities are kept - no filtering. Missing data is stored as null.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
HTTP_TIMEOUT_SECONDS = 60

# Rate limiting - keeps us within Wikidata's limits
BATCH_SIZE = 50
DELAY_BETWEEN_BATCHES = 1.0
MAX_RETRIES = 3
RETRY_DELAY = 2.0

_HEADERS = {
    "User-Agent": "CDS-CityFetch/2.1 (github.com/filip CDS-CityFetch; filip.dvorak13@gmail.com)",
    "Accept": "text/csv; charset=utf-8",
}


@dataclass
class CityData:
    """Represents a city record."""
    wikidata_id: str
    city_name: str
    language: str
    latitude: float
    longitude: float
    country: Optional[str] = None
    country_code: Optional[str] = None
    admin_region: Optional[str] = None
    population: Optional[int] = None


def _execute_query(query: str, language: str, batch_name: str) -> list[dict]:
    """Execute SPARQL query with retry logic."""
    delay = RETRY_DELAY
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with httpx.Client(headers=_HEADERS, timeout=HTTP_TIMEOUT_SECONDS) as client:
                response = client.post(SPARQL_ENDPOINT, data={"query": query})
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get("retry-after", delay))
                    logger.warning(f"[{language}] {batch_name} - Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    delay = min(delay * 2, 30)
                    continue
                
                if response.status_code >= 500:
                    logger.warning(f"[{language}] {batch_name} - Server error {response.status_code}, retrying...")
                    time.sleep(delay)
                    delay = min(delay * 2, 30)
                    continue
                
                if not response.is_success:
                    logger.error(f"[{language}] {batch_name} - Failed: HTTP {response.status_code}")
                    return []
                
                csv_text = response.text.replace('\r\n', '\n').replace('\r', '\n')
                return list(csv.DictReader(io.StringIO(csv_text)))
                
        except Exception as exc:
            logger.warning(f"[{language}] {batch_name} - Error: {exc}, retrying...")
            time.sleep(delay)
            delay = min(delay * 2, 30)
    
    logger.error(f"[{language}] {batch_name} - All retries exhausted")
    return []


def _chunk(items: list, size: int) -> list[list]:
    """Split list into chunks."""
    return [items[i:i + size] for i in range(0, len(items), size)]


def fetch_cities(language: str) -> list[CityData]:
    """
    Fetch all city data for a language using multi-pass approach.
    
    This is the main and only method - always fetches maximum data.
    Takes ~20-25 minutes per language but gets all available data.
    """
    logger.info(f"[{language}] Starting maximum data fetch (~20-25 minutes)...")
    
    # ========================================================================
    # PASS 1: Core Data
    # ========================================================================
    logger.info(f"[{language}] Pass 1/5: Core data (id, name, coordinates)...")
    
    query = f"""
    SELECT ?city ?label ?lat ?lon WHERE {{
      ?city wdt:P31 wd:Q515 .
      ?city wdt:P625 ?coord .
      ?city rdfs:label ?label .
      FILTER(LANG(?label) = "{language}" || LANG(?label) = "en")
      BIND(geof:latitude(?coord) AS ?lat)
      BIND(geof:longitude(?coord) AS ?lon)
    }}
    ORDER BY ASC(?city)"""
    
    rows = _execute_query(query, language, "core")
    if not rows:
        logger.error(f"[{language}] Failed to fetch core data")
        return []
    
    cities = {}
    for row in rows:
        try:
            city_uri = row.get("city", "").strip()
            if not city_uri:
                continue
            
            cities[city_uri.rsplit("/", 1)[-1]] = CityData(
                wikidata_id=city_uri.rsplit("/", 1)[-1],
                city_name=row.get("label", "").strip(),
                language=language,
                latitude=float(row.get("lat", 0)),
                longitude=float(row.get("lon", 0)),
            )
        except Exception:
            continue
    
    total = len(cities)
    logger.info(f"[{language}] Pass 1 complete: {total} cities")
    
    if not cities:
        return []
    
    city_ids = list(cities.keys())
    
    # ========================================================================
    # PASS 2: Country
    # ========================================================================
    logger.info(f"[{language}] Pass 2/5: Country data...")
    
    batches = _chunk(city_ids, BATCH_SIZE)
    failed = 0
    
    for i, batch in enumerate(batches, 1):
        values = " ".join(f"wd:{qid}" for qid in batch)
        query = f"""
        SELECT ?city ?countryLabel WHERE {{
          VALUES ?city {{ {values} }}
          OPTIONAL {{
            ?city wdt:P17 ?country .
            ?country rdfs:label ?countryLabel .
            FILTER(LANG(?countryLabel) = "{language}" || LANG(?countryLabel) = "en")
          }}
        }}"""
        
        rows = _execute_query(query, language, f"country-{i}/{len(batches)}")
        if rows:
            for row in rows:
                try:
                    qid = row.get("city", "").rsplit("/", 1)[-1]
                    if qid in cities and (val := row.get("countryLabel", "").strip()):
                        cities[qid].country = val
                except Exception:
                    continue
        else:
            failed += 1
        
        if i % 10 == 0 or i == len(batches):
            with_country = sum(1 for c in cities.values() if c.country)
            logger.info(f"[{language}] Country: {i}/{len(batches)} batches, {with_country}/{total} cities")
        
        if i < len(batches):
            time.sleep(DELAY_BETWEEN_BATCHES)
    
    with_country = sum(1 for c in cities.values() if c.country)
    logger.info(f"[{language}] Pass 2 complete: {with_country}/{total} cities have country ({failed} failed batches)")
    
    # ========================================================================
    # PASS 3: Population
    # ========================================================================
    logger.info(f"[{language}] Pass 3/5: Population data...")
    
    batches = _chunk(city_ids, BATCH_SIZE)
    failed = 0
    
    for i, batch in enumerate(batches, 1):
        values = " ".join(f"wd:{qid}" for qid in batch)
        query = f"""
        SELECT ?city ?pop WHERE {{
          VALUES ?city {{ {values} }}
          OPTIONAL {{ ?city wdt:P1082 ?pop }}
        }}"""
        
        rows = _execute_query(query, language, f"pop-{i}/{len(batches)}")
        if rows:
            for row in rows:
                try:
                    qid = row.get("city", "").rsplit("/", 1)[-1]
                    if qid in cities and (val := row.get("pop", "").strip()):
                        cities[qid].population = int(float(val))
                except (ValueError, TypeError):
                    continue
        else:
            failed += 1
        
        if i % 10 == 0 or i == len(batches):
            with_pop = sum(1 for c in cities.values() if c.population)
            logger.info(f"[{language}] Population: {i}/{len(batches)} batches, {with_pop}/{total} cities")
        
        if i < len(batches):
            time.sleep(DELAY_BETWEEN_BATCHES)
    
    with_pop = sum(1 for c in cities.values() if c.population)
    logger.info(f"[{language}] Pass 3 complete: {with_pop}/{total} cities have population")
    
    # ========================================================================
    # PASS 4: Country Code
    # ========================================================================
    logger.info(f"[{language}] Pass 4/5: Country codes...")
    
    # Get unique country entities that we found
    country_qids = set()
    for city in cities.values():
        if city.country:
            # We need the country QID, but we only have the name
            # For now, we'll skip this pass - it requires mapping names back to QIDs
            pass
    
    logger.info(f"[{language}] Pass 4: Country code lookup requires QID mapping (skipped)")
    
    # ========================================================================
    # PASS 5: Admin Region
    # ========================================================================
    logger.info(f"[{language}] Pass 5/5: Admin region data...")
    
    batches = _chunk(city_ids, BATCH_SIZE)
    failed = 0
    
    for i, batch in enumerate(batches, 1):
        values = " ".join(f"wd:{qid}" for qid in batch)
        query = f"""
        SELECT ?city ?adminLabel WHERE {{
          VALUES ?city {{ {values} }}
          OPTIONAL {{
            ?city wdt:P131 ?admin .
            ?admin rdfs:label ?adminLabel .
            FILTER(LANG(?adminLabel) = "{language}" || LANG(?adminLabel) = "en")
          }}
        }}"""
        
        rows = _execute_query(query, language, f"admin-{i}/{len(batches)}")
        if rows:
            for row in rows:
                try:
                    qid = row.get("city", "").rsplit("/", 1)[-1]
                    if qid in cities and (val := row.get("adminLabel", "").strip()):
                        cities[qid].admin_region = val
                except Exception:
                    continue
        else:
            failed += 1
        
        if i % 10 == 0 or i == len(batches):
            with_admin = sum(1 for c in cities.values() if c.admin_region)
            logger.info(f"[{language}] Admin: {i}/{len(batches)} batches, {with_admin}/{total} cities")
        
        if i < len(batches):
            time.sleep(DELAY_BETWEEN_BATCHES)
    
    with_admin = sum(1 for c in cities.values() if c.admin_region)
    logger.info(f"[{language}] Pass 5 complete: {with_admin}/{total} cities have admin region")
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    result = list(cities.values())
    
    logger.info(f"[{language}] === FETCH COMPLETE ===")
    logger.info(f"[{language}] Total cities: {len(result)}")
    logger.info(f"[{language}] With country: {with_country} ({100*with_country//len(result)}%)")
    logger.info(f"[{language}] With population: {with_pop} ({100*with_pop//len(result)}%)")
    logger.info(f"[{language}] With admin region: {with_admin} ({100*with_admin//len(result)}%)")
    
    return result
