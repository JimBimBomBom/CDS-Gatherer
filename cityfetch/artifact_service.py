"""
artifact_service.py
-------------------
Handles OCI artifact operations with GHCR for city data storage.

Stores each language's city data as an OCI artifact in GHCR.
Keeps latest + previous version for backup.
Implements upsert logic: merge new data with existing, keeping non-null values.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from cityfetch.wikidata_service import CityData

logger = logging.getLogger(__name__)

# GHCR configuration
GHCR_REGISTRY = "ghcr.io"
GHCR_REPOSITORY = os.environ.get("GHCR_REPOSITORY", "filip/cityfetch-data")


def _get_artifact_reference(language: str, tag: str = "latest") -> str:
    """Get full OCI reference for language artifact."""
    return f"{GHCR_REGISTRY}/{GHCR_REPOSITORY}/{language}:{tag}"


def pull_language_data(language: str) -> Optional[list[CityData]]:
    """
    Pull existing city data for a language from GHCR.
    
    Returns None if no data exists or pull fails.
    Public pulls don't require authentication.
    """
    try:
        ref = _get_artifact_reference(language)
        logger.info(f"[artifact] Pulling existing data for {language} from {ref}")
        
        # Try to pull using oras CLI
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / f"{language}_cities.json"
            
            result = subprocess.run(
                ["oras", "pull", ref, "-o", str(tmpdir)],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                if "not found" in result.stderr.lower() or "404" in result.stderr:
                    logger.info(f"[artifact] No existing data for {language} (will create new)")
                    return None
                logger.warning(f"[artifact] Failed to pull {language}: {result.stderr}")
                return None
            
            # Find and parse the pulled file
            json_files = list(Path(tmpdir).glob("*.json"))
            if not json_files:
                logger.warning(f"[artifact] No JSON files found in pulled artifact for {language}")
                return None
            
            with open(json_files[0], "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Convert to CityData objects
            cities = []
            for city_dict in data.get("cities", []):
                cities.append(CityData(
                    wikidata_id=city_dict["city_id"],
                    city_name=city_dict["city_name"],
                    language=city_dict["language"],
                    latitude=city_dict["latitude"],
                    longitude=city_dict["longitude"],
                    country=city_dict.get("country"),
                    country_code=city_dict.get("country_code"),
                    admin_region=city_dict.get("admin_region"),
                    population=city_dict.get("population"),
                ))
            
            logger.info(f"[artifact] Pulled {len(cities)} cities for {language}")
            return cities
            
    except FileNotFoundError:
        logger.warning("[artifact] oras CLI not found. Cannot pull from GHCR.")
        return None
    except Exception as exc:
        logger.warning(f"[artifact] Error pulling {language}: {exc}")
        return None


def push_language_data(
    language: str,
    cities: list[CityData],
    output_dir: Path,
    token: Optional[str] = None
) -> bool:
    """
    Push city data for a language to GHCR as OCI artifact.
    
    Pushes with two tags:
    - 'latest': Always points to newest data
    - 'previous': Points to what was 'latest' before this push
    
    Requires GHCR_TOKEN environment variable or token parameter for push.
    """
    try:
        if not token:
            token = os.environ.get("GHCR_TOKEN")
        
        if not token:
            logger.error("[artifact] No GHCR_TOKEN provided. Cannot push to GHCR.")
            return False
        
        # First, pull existing 'latest' and tag it as 'previous'
        logger.info(f"[artifact] Tagging existing latest as previous for {language}")
        _retag_existing(language, token)
        
        # Prepare the JSON file
        json_data = {
            "metadata": {
                "language": language,
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "Wikidata",
                "tool": "CDS-CityFetch",
                "total_records": len(cities),
            },
            "cities": [
                {
                    "city_id": c.wikidata_id,
                    "city_name": c.city_name,
                    "language": c.language,
                    "latitude": c.latitude,
                    "longitude": c.longitude,
                    "country": c.country,
                    "country_code": c.country_code,
                    "admin_region": c.admin_region,
                    "population": c.population,
                }
                for c in cities
            ]
        }
        
        with tempfile.TemporaryDirectory() as tmpdir:
            json_file = Path(tmpdir) / f"{language}_cities.json"
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            
            ref = _get_artifact_reference(language)
            logger.info(f"[artifact] Pushing {len(cities)} cities for {language} to {ref}")
            
            # Login to GHCR
            login_result = subprocess.run(
                ["echo", token, "|", "oras", "login", GHCR_REGISTRY, "-u", "GITHUB_ACTOR", "--password-stdin"],
                capture_output=True,
                text=True,
                shell=True,
                timeout=30
            )
            
            if login_result.returncode != 0:
                logger.error(f"[artifact] GHCR login failed: {login_result.stderr}")
                return False
            
            # Push the artifact
            result = subprocess.run(
                ["oras", "push", ref, str(json_file), "--artifact-type", "application/cityfetch-data"],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode != 0:
                logger.error(f"[artifact] Push failed: {result.stderr}")
                return False
            
            logger.info(f"[artifact] Successfully pushed {language} to GHCR")
            return True
            
    except FileNotFoundError:
        logger.error("[artifact] oras CLI not found. Cannot push to GHCR.")
        return False
    except Exception as exc:
        logger.error(f"[artifact] Error pushing {language}: {exc}")
        return False


def _retag_existing(language: str, token: str) -> None:
    """Tag existing 'latest' as 'previous' before pushing new data."""
    try:
        ref_latest = _get_artifact_reference(language, "latest")
        ref_previous = _get_artifact_reference(language, "previous")
        
        # Pull manifest of latest
        result = subprocess.run(
            ["oras", "manifest", "fetch", ref_latest],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            # Tag it as previous
            subprocess.run(
                ["oras", "tag", ref_latest, ref_previous.split(":")[-1]],
                capture_output=True,
                timeout=30
            )
            logger.info(f"[artifact] Tagged existing latest as previous for {language}")
    except Exception:
        # Ignore errors - if there's no existing data, that's fine
        pass


def merge_city_data(existing: list[CityData], new: list[CityData]) -> list[CityData]:
    """
    Merge existing and new city data using upsert logic.
    
    Rules:
    1. If city exists in both: keep non-null fields from both
       - New non-null value replaces old null value
       - Old non-null value is kept if new value is null
    2. If city only in new: add it
    3. If city only in existing: keep it
    
    Returns merged list with all cities.
    """
    # Create lookup by wikidata_id
    existing_map = {c.wikidata_id: c for c in existing}
    new_map = {c.wikidata_id: c for c in new}
    
    merged: dict[str, CityData] = {}
    
    # Process all unique city IDs
    all_ids = set(existing_map.keys()) | set(new_map.keys())
    
    for city_id in all_ids:
        if city_id in new_map and city_id in existing_map:
            # City in both - merge fields
            old = existing_map[city_id]
            new = new_map[city_id]
            
            merged[city_id] = CityData(
                wikidata_id=city_id,
                city_name=new.city_name or old.city_name,  # Prefer new name
                language=new.language,
                latitude=new.latitude if new.latitude != 0 else old.latitude,
                longitude=new.longitude if new.longitude != 0 else old.longitude,
                country=new.country or old.country,  # Keep non-null
                country_code=new.country_code or old.country_code,
                admin_region=new.admin_region or old.admin_region,
                population=new.population or old.population,  # Keep non-null
            )
        elif city_id in new_map:
            # City only in new data
            merged[city_id] = new_map[city_id]
        else:
            # City only in existing data
            merged[city_id] = existing_map[city_id]
    
    logger.info(f"[merge] Merged: {len(existing)} existing + {len(new)} new = {len(merged)} total")
    logger.info(f"[merge] New cities added: {len(new_map - existing_map)}")
    logger.info(f"[merge] Cities updated: {len(new_map & existing_map)}")
    
    return list(merged.values())
