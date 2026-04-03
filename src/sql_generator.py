"""
sql_generator.py
----------------
Builds the cities.sql file from a list of SparqlCityInfo objects.

SQL format mirrors the C# DataGenerationService.GenerateSqlFileAsync:
  - INSERT INTO cities (...) VALUES ... ON DUPLICATE KEY UPDATE ...
  - 1 000-row batches
  - Atomic write: write to a timestamped temp file, then rename to cities.sql
  - USE CityDistanceService; header so the file is self-contained
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from wikidata_service import SparqlCityInfo

logger = logging.getLogger(__name__)

BATCH_SIZE = 1_000
_DEFAULT_DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
SQL_FILENAME = "cities.sql"


def _escape_sql(value: str) -> str:
    """Escape single quotes and backslashes for a MySQL string literal."""
    return value.replace("\\", "\\\\").replace("'", "''")


def _sql_str(value: Optional[str]) -> str:
    """Return a quoted SQL string or NULL."""
    if value is None:
        return "NULL"
    return f"'{_escape_sql(value)}'"


def _sql_int(value: Optional[int]) -> str:
    """Return an integer literal or NULL."""
    return "NULL" if value is None else str(value)


def _write_batch(lines: list[str], batch: list[SparqlCityInfo]) -> None:
    """Append an INSERT ... ON DUPLICATE KEY UPDATE block for *batch* to *lines*."""
    lines.append(
        "INSERT INTO cities "
        "(CityId, CityName, Latitude, Longitude, CountryCode, Country, AdminRegion, Population) "
        "VALUES"
    )

    for i, city in enumerate(batch):
        comma = "" if i == len(batch) - 1 else ","
        row = (
            f"    ({_sql_str(city.wikidata_id)}, {_sql_str(city.city_name)}, "
            f"{city.latitude:.8f}, {city.longitude:.8f}, "
            f"{_sql_str(city.country_code)}, {_sql_str(city.country)}, "
            f"{_sql_str(city.admin_region)}, {_sql_int(city.population)}){comma}"
        )
        lines.append(row)

    lines.append("ON DUPLICATE KEY UPDATE")
    lines.append("    CityName = VALUES(CityName),")
    lines.append("    Latitude = VALUES(Latitude),")
    lines.append("    Longitude = VALUES(Longitude),")
    lines.append("    CountryCode = VALUES(CountryCode),")
    lines.append("    Country = VALUES(Country),")
    lines.append("    AdminRegion = VALUES(AdminRegion),")
    lines.append("    Population = VALUES(Population);")
    lines.append("")  # blank line between batches


def generate_sql_file(cities: list[SparqlCityInfo], data_dir: str | None = None) -> str:
    """
    Deduplicate *cities* by wikidata_id (first-seen wins, preserving the
    language ordering the caller used), then write the SQL file.

    *data_dir* overrides the ``DATA_DIR`` environment variable when provided.

    Returns the final path of the written file.
    """
    target_dir = data_dir if data_dir is not None else _DEFAULT_DATA_DIR
    os.makedirs(target_dir, exist_ok=True)

    # Deduplicate: one SQL row per WikidataId (first occurrence wins)
    seen: set[str] = set()
    unique: list[SparqlCityInfo] = []
    for city in cities:
        if city.wikidata_id not in seen:
            seen.add(city.wikidata_id)
            unique.append(city)

    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.strftime("%Y%m%d_%H%M%S")
    temp_path = os.path.join(target_dir, f"cities_{timestamp}.sql")
    final_path = os.path.join(target_dir, SQL_FILENAME)

    logger.info(
        "Generating SQL file: %d unique cities (from %d total records).",
        len(unique), len(cities),
    )

    lines: list[str] = [
        "-- Auto-generated SQL file from Wikidata",
        f"-- Generated at: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"-- Total records: {len(unique)}",
        "-- Format: INSERT ... ON DUPLICATE KEY UPDATE",
        "",
        "USE CityDistanceService;",
        "",
    ]

    for batch_start in range(0, len(unique), BATCH_SIZE):
        batch = unique[batch_start : batch_start + BATCH_SIZE]
        _write_batch(lines, batch)

    sql_content = "\n".join(lines)

    try:
        with open(temp_path, "w", encoding="utf-8") as fh:
            fh.write(sql_content)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise

    # Atomic rename (os.replace is atomic on POSIX; on Windows it overwrites)
    os.replace(temp_path, final_path)

    logger.info("SQL file written to: %s", final_path)
    return final_path
