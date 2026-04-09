#!/usr/bin/env python3
"""
CDS-CityFetch
-------------
Fetches city data from Wikidata for all predefined languages
and stores them as separate JSON files.

Usage:
    docker run --rm -v ./output:/data cityfetch
    docker run --rm cityfetch version
    docker run --rm cityfetch --help
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cityfetch import __version__ as VERSION
from cityfetch.wikidata_service import fetch_cities, CityData
from cityfetch.language_service import LANGUAGE_CODES
from cityfetch.artifact_service import (
    pull_language_data,
    push_language_data,
    merge_city_data,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

__description__ = "Fetch city data from Wikidata for all languages"


def city_to_dict(city: CityData) -> dict[str, Any]:
    """Convert a SparqlCityInfo to a dictionary."""
    return {
        "city_id": city.wikidata_id,
        "city_name": city.city_name,
        "language": city.language,
        "latitude": city.latitude,
        "longitude": city.longitude,
        "country": city.country,
        "country_code": city.country_code,
        "admin_region": city.admin_region,
        "population": city.population,
    }


def save_language_file(
    cities: list[CityData],
    language: str,
    output_dir: Path,
    fetched_at: str,
) -> tuple[Path, int]:
    """
    Save cities for a single language to a JSON file.
    
    All cities are saved, including those with incomplete data.
    Missing fields are stored as null.
    
    Returns:
        Tuple of (file_path, record_count)
    """
    # Build the document structure
    document = {
        "metadata": {
            "language": language,
            "fetched_at": fetched_at,
            "source": "Wikidata",
            "tool": "CDS-CityFetch",
            "tool_version": VERSION,
            "total_records": len(cities),
        },
        "cities": [city_to_dict(city) for city in cities],
    }
    
    # Write file
    output_file = output_dir / f"{language}_cities.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(document, f, indent=2, ensure_ascii=False)
    
    logger.info("Saved %d cities to %s", len(cities), output_file.name)
    return output_file, len(cities)


def save_manifest(
    language_stats: dict[str, dict],
    output_dir: Path,
    generated_at: str,
) -> Path:
    """
    Save manifest.json with summary of all languages.
    
    Args:
        language_stats: Dict mapping language code to stats dict
        output_dir: Directory to write manifest
        generated_at: ISO timestamp string
    """
    total_records = sum(stats["record_count"] for stats in language_stats.values())
    
    manifest = {
        "generated_at": generated_at,
        "source": "Wikidata",
        "tool": "CDS-CityFetch",
        "tool_version": VERSION,
        "total_languages": len(language_stats),
        "total_records": total_records,
        "languages": language_stats,
    }
    
    manifest_file = output_dir / "manifest.json"
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    
    logger.info("Saved manifest with %d languages", len(language_stats))
    return manifest_file


def fetch_all_languages(output_dir: Path) -> dict[str, dict]:
    """
    Fetch city data for all languages and save to separate files.
    
    Returns:
        Dict mapping language code to stats (for manifest)
    """
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    language_stats: dict[str, dict] = {}
    
    total_languages = len(LANGUAGE_CODES)
    logger.info("Starting fetch for %d languages", total_languages)
    
    for idx, language in enumerate(LANGUAGE_CODES, 1):
        logger.info("[%d/%d] Fetching cities for language: %s", idx, total_languages, language)
        
        try:
            # Fetch cities using the original wikidata_service
            cities = fetch_cities(language)
            
            if not cities:
                logger.warning("No cities returned for language: %s", language)
                continue
            
            # Save to file
            output_file, record_count = save_language_file(
                cities, language, output_dir, generated_at
            )
            
            # Record stats for manifest
            language_stats[language] = {
                "file": output_file.name,
                "record_count": record_count,
                "fetched_at": generated_at,
            }
            
            logger.info("[%d/%d] Completed %s: %d cities", idx, total_languages, language, record_count)
            
        except Exception as exc:
            logger.error("[%d/%d] Failed to fetch for language %s: %s", idx, total_languages, language, exc)
            # Continue with other languages even if one fails
            continue
    
    return language_stats


def update_artifacts_for_all_languages() -> dict[str, dict]:
    """
    Fetch data for all languages and update GHCR artifacts.
    
    For each language:
    1. Pull existing data from GHCR (if exists)
    2. Fetch fresh data from Wikidata
    3. Merge: upsert (keep non-null values from both)
    4. Push merged data to GHCR as 'latest'
    5. Also save to local output directory
    
    Returns dict mapping language to stats.
    """
    from pathlib import Path
    
    output_dir = Path(os.environ.get("OUTPUT_DIR", "/data"))
    output_dir.mkdir(parents=True, exist_ok=True)
    
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    language_stats: dict[str, dict] = {}
    
    total_languages = len(LANGUAGE_CODES)
    logger.info("[artifact-update] Starting update for %d languages", total_languages)
    logger.info("[artifact-update] This will take approximately %d hours", total_languages * 25 // 60)
    
    for idx, language in enumerate(LANGUAGE_CODES, 1):
        logger.info("[%d/%d] Processing language: %s", idx, total_languages, language)
        
        try:
            # Step 1: Pull existing data from GHCR
            logger.info("[%s] Step 1: Pulling existing data from GHCR...", language)
            existing_cities = pull_language_data(language)
            
            if existing_cities:
                logger.info("[%s] Found %d existing cities in GHCR", language, len(existing_cities))
            else:
                logger.info("[%s] No existing data in GHCR (will create new)", language)
            
            # Step 2: Fetch fresh data from Wikidata
            logger.info("[%s] Step 2: Fetching fresh data from Wikidata...", language)
            new_cities = fetch_cities(language)
            
            if not new_cities:
                logger.warning("[%s] No cities returned from Wikidata, skipping", language)
                continue
            
            logger.info("[%s] Fetched %d cities from Wikidata", language, len(new_cities))
            
            # Step 3: Merge data (upsert)
            if existing_cities:
                logger.info("[%s] Step 3: Merging existing + new data...", language)
                merged_cities = merge_city_data(existing_cities, new_cities)
            else:
                logger.info("[%s] Step 3: No existing data, using fresh fetch only", language)
                merged_cities = new_cities
            
            logger.info("[%s] Merged result: %d cities total", language, len(merged_cities))
            
            # Step 4: Push to GHCR
            logger.info("[%s] Step 4: Pushing to GHCR...", language)
            # Get token from environment
            token = os.environ.get("GHCR_TOKEN")
            if token:
                success = push_language_data(language, merged_cities, output_dir, token)
                if success:
                    logger.info("[%s] Successfully pushed to GHCR", language)
                else:
                    logger.error("[%s] Failed to push to GHCR", language)
            else:
                logger.warning("[%s] No GHCR_TOKEN, skipping push to GHCR", language)
            
            # Step 5: Save to local file
            output_file, record_count = save_language_file(
                merged_cities, language, output_dir, generated_at
            )
            
            # Record stats
            language_stats[language] = {
                "file": output_file.name,
                "record_count": record_count,
                "fetched_at": generated_at,
                "pushed_to_ghcr": bool(token and success) if 'success' in dir() else False,
            }
            
            logger.info("[%d/%d] Completed %s: %d cities", idx, total_languages, language, record_count)
            
        except Exception as exc:
            logger.error("[%d/%d] Failed to process language %s: %s", idx, total_languages, language, exc)
            continue
    
    return language_stats


def show_version() -> None:
    """Display version information."""
    print(f"CDS-CityFetch {VERSION}")
    print(__description__)
    print()
    print("City data source: Wikidata (https://www.wikidata.org)")
    print("Output format: JSON (one file per language)")


def show_help() -> None:
    """Display help information."""
    print(f"CDS-CityFetch {VERSION}")
    print()
    print("Usage:")
    print("    docker run --rm -v ./output:/data cityfetch")
    print("    docker run --rm cityfetch version")
    print("    docker run --rm cityfetch --help")
    print()
    print("Commands:")
    print("    (no args)     Fetch city data for all languages")
    print("    version       Show version information")
    print("    --help        Show this help message")
    print()
    print("Environment Variables:")
    print("    OUTPUT_DIR    Output directory (default: /data)")
    print()
    print("Data Source: Wikidata (https://www.wikidata.org)")
    print("License: CC0")


def main() -> None:
    """Main entry point."""
    # Handle help command
    if len(sys.argv) > 1 and sys.argv[1] in ("--help", "-h", "help"):
        show_help()
        return
    
    # Handle version command
    if len(sys.argv) > 1 and sys.argv[1] in ("--version", "-v", "version"):
        show_version()
        return
    
    # Check for artifact update mode
    if len(sys.argv) > 1 and sys.argv[1] == "--mode=update-artifacts":
        logger.info("=" * 60)
        logger.info("CDS-CityFetch %s - Artifact Update Mode", VERSION)
        logger.info("=" * 60)
        
        language_stats = update_artifacts_for_all_languages()
        
        if not language_stats:
            logger.error("No languages were successfully processed. Exiting.")
            sys.exit(1)
        
        # Save manifest
        output_dir = Path(os.environ.get("OUTPUT_DIR", "/data"))
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        manifest_file = save_manifest(language_stats, output_dir, generated_at)
        
        # Summary
        total_records = sum(stats["record_count"] for stats in language_stats.values())
        pushed_count = sum(1 for s in language_stats.values() if s.get("pushed_to_ghcr"))
        logger.info("=" * 60)
        logger.info("Artifact update complete!")
        logger.info("Languages: %d", len(language_stats))
        logger.info("Pushed to GHCR: %d", pushed_count)
        logger.info("Total records: %d", total_records)
        logger.info("=" * 60)
        return
    
    # Default mode: fetch and save locally only
    output_dir = Path(os.environ.get("OUTPUT_DIR", "/data"))
    
    # Check if output directory is properly mounted (not just the container's /data)
    # We do this by checking if we can write a test file
    try:
        test_file = output_dir / ".write_test"
        test_file.touch()
        test_file.unlink()
        is_writable = True
    except (OSError, PermissionError):
        is_writable = False
    
    # If not writable and it's the default /data, show help
    if not is_writable and str(output_dir) == "/data":
        print("Error: Output directory is not writable.")
        print()
        print("Did you forget to mount a volume?")
        print()
        show_help()
        sys.exit(1)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("CDS-CityFetch %s", VERSION)
    logger.info("Output directory: %s", output_dir)
    logger.info("=" * 60)
    
    # Fetch all languages
    language_stats = fetch_all_languages(output_dir)
    
    if not language_stats:
        logger.error("No languages were successfully fetched. Exiting.")
        sys.exit(1)
    
    # Save manifest
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest_file = save_manifest(language_stats, output_dir, generated_at)
    
    # Summary
    total_records = sum(stats["record_count"] for stats in language_stats.values())
    logger.info("=" * 60)
    logger.info("Fetch complete!")
    logger.info("Languages: %d", len(language_stats))
    logger.info("Total records: %d", total_records)
    logger.info("Output directory: %s", output_dir)
    logger.info("Files created: %d language files + manifest.json", len(language_stats))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
