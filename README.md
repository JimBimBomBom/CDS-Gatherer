# CDS-CityFetch

Minimal Docker service that fetches city data from [Wikidata](https://www.wikidata.org) and exports it to JSON format (one file per language).

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Available Commands](#available-commands)
- [Docker Usage](#docker-usage)
  - [Windows (PowerShell)](#windows-powershell)
  - [Linux/macOS (Bash)](#linuxmacos-bash)
- [Output](#output)
- [Importing to Databases](#importing-to-databases)
- [Languages](#languages)
- [Environment Variables](#environment-variables)
- [Architecture](#architecture)
- [Versioning & Releases](#versioning--releases)
- [Development](#development)
- [Data Source](#data-source)
- [License](#license)

---

## Prerequisites

### 1. Install Docker

Before using this tool, you need Docker installed:

- **Windows**: Download [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)
- **macOS**: Download [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/)
- **Linux**: Install via your package manager (e.g., `sudo apt install docker.io` on Ubuntu)

Verify Docker is running:
```bash
docker --version
docker run hello-world
```

---

## Quick Start

### 1. Build the Docker image

```bash
docker build -t cityfetch .
```

### 2. Run and fetch data

**Windows (PowerShell):**
```powershell
mkdir -Force output
docker run --rm -v ${PWD}/output:/data cityfetch
```

**Linux/macOS (Bash):**
```bash
mkdir -p output
docker run --rm -v $(pwd)/output:/data cityfetch
```

---

## Available Commands

The tool supports the following commands:

### 1. Default (no arguments) - Fetch all cities

**Purpose:** Fetch city data for all 65 languages from Wikidata and save to JSON files.

**Windows (PowerShell):**
```powershell
mkdir -Force output
docker run --rm -v ${PWD}/output:/data cityfetch
```

**Linux/macOS (Bash):**
```bash
mkdir -p output
docker run --rm -v $(pwd)/output:/data cityfetch
```

**Note:** The `-v` flag is required to persist output files. Without it, data is written to the container only and lost when the container exits.

### 2. `version` - Show version information

**Purpose:** Display the current version of CDS-CityFetch.

```bash
docker run --rm cityfetch version
```

### 3. `--help` - Show help

**Purpose:** Display usage information and available commands.

```bash
docker run --rm cityfetch --help
```

---

## Versioning & Releases

CDS-CityFetch uses **Semantic Versioning (X.Y.Z)** with strict immutability:

### Version Rules
- **Immutable Versions**: Once a version is released (e.g., `2.1.0`), it can never be changed
- **Git Tag Match**: The git tag MUST match the version in `cityfetch/__init__.py`
- **Auto-Enforcement**: CI/CD blocks releases if version rules are violated

### Releasing a New Version

1. **Update the version** in `cityfetch/__init__.py`:
   ```python
   __version__ = "2.1.0"  # Bump this
   ```

2. **Commit and push** to main:
   ```bash
   git add cityfetch/__init__.py
   git commit -m "Bump version to 2.1.0"
   git push origin main
   ```

3. **Create a git tag** (triggers release):
   ```bash
   git tag 2.1.0
   git push origin 2.1.0
   ```

The CI/CD will:
- Verify the tag matches the code version
- Check the version hasn't been released before
- Build and push the Docker image
- Create a GitHub release

### CI/CD Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `version-check.yml` | PR to main | Blocks merge if version not bumped or already exists |
| `release.yml` | Git tag push | Builds Docker image, verifies immutability |

### Version Bump Required

Every PR must bump the version. The CI will fail if:
- Version matches the base branch (no bump)
- Version already has a git tag (immutable)
- Version doesn't follow semantic versioning (X.Y.Z)

### 2. Default (no arguments) - Fetch all city data

**Purpose:** Fetch city data for all 70+ languages from Wikidata.

**Windows (PowerShell):**
```powershell
mkdir -Force output
docker run --rm -v ${PWD}/output:/data cityfetch
```

**Linux/macOS (Bash):**
```bash
mkdir -p output
docker run --rm -v $(pwd)/output:/data cityfetch
```

**What it does:**
- Fetches city data for 70+ languages from Wikidata
- Creates one JSON file per language (e.g., `en_cities.json`, `de_cities.json`)
- Generates a `manifest.json` with summary statistics
- Exits when complete (this is a one-shot container)

---

## Docker Usage

### Windows (PowerShell)

On Windows PowerShell, use `${PWD}` instead of `$(pwd)`:

```powershell
# Build image
docker build -t cityfetch .

# Create output directory
mkdir -Force output

# Run and fetch all data
docker run --rm -v ${PWD}/output:/data cityfetch

# Check version
docker run --rm cityfetch python main.py version

# Custom output directory
docker run --rm -e OUTPUT_DIR=/output -v ${PWD}/mydata:/output cityfetch
```

### Linux/macOS (Bash)

On Linux and macOS, use `$(pwd)`:

```bash
# Build image
docker build -t cityfetch .

# Create output directory
mkdir -p output

# Run and fetch all data
docker run --rm -v $(pwd)/output:/data cityfetch

# Check version
docker run --rm cityfetch python main.py version

# Custom output directory
docker run --rm -e OUTPUT_DIR=/output -v $(pwd)/mydata:/output cityfetch
```

---

## Output

The container generates the following files in the output directory:

```
output/
├── en_cities.json     # English cities
├── de_cities.json     # German cities
├── fr_cities.json     # French cities
├── ...                # 70+ other languages
└── manifest.json      # Summary with record counts
```

### manifest.json structure

```json
{
  "generated_at": "2026-04-08T14:30:00Z",
  "source": "Wikidata",
  "tool": "CDS-CityFetch",
  "tool_version": "2.0.0",
  "total_languages": 70,
  "total_records": 350000,
  "languages": {
    "en": {
      "file": "en_cities.json",
      "record_count": 87432,
      "fetched_at": "2026-04-08T14:30:00Z"
    }
  }
}
```

### City record structure (within each language file)

```json
{
  "metadata": {
    "language": "en",
    "fetched_at": "2026-04-08T14:30:00Z",
    "total_records": 87432
  },
  "cities": [
    {
      "city_id": "Q90",
      "city_name": "Paris",
      "language": "en",
      "latitude": 48.85341,
      "longitude": 2.3488,
      "country": "France",
      "country_code": "FR",
      "admin_region": "Île-de-France",
      "population": 2161000
    }
  ]
}
```

---

## Importing to Databases

The JSON format is compatible with all major databases:

**MySQL:**
```sql
LOAD DATA INFILE '/data/en_cities.json' INTO TABLE cities (@json)
SET city_id = JSON_UNQUOTE(JSON_EXTRACT(@json, '$.city_id'));
```

**PostgreSQL:**
```sql
COPY (SELECT * FROM jsonb_array_elements(
  (SELECT data->'cities' FROM temp_import)
)) TO cities;
```

**MongoDB:**
```bash
mongoimport --db mydb --collection cities --file en_cities.json --jsonArray
```

---

## Languages

Data is fetched for 70+ languages including:

- **Global:** English, Chinese, Spanish, Arabic, Hindi, Portuguese, Russian, Japanese
- **European:** German, French, Italian, Dutch, Polish, Swedish, Turkish, Greek, Czech, Hungarian, Romanian, etc.
- **Asian:** Korean, Vietnamese, Thai, Indonesian, Bengali, Tamil, Telugu, Urdu, Persian
- **Other:** Swahili, Afrikaans, Esperanto, Latin

See `cityfetch/language_service.py` for the complete list.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OUTPUT_DIR` | `/data` | Output directory inside container |

---

## Architecture

This is a **one-shot container** - it runs once, fetches all data, writes files, and exits.

```
Docker Run
    ↓
main.py
    ↓
For each language:
    ↓
wikidata_service.fetch_cities()
    ├── Pass 1: Core data (id, name, lat, lon)
    ├── Pass 2: Country (batched)
    ├── Pass 3: Population (batched)
    └── Pass 4: Admin region (batched)
    ↓
Save JSON file
    ↓
Generate manifest.json
    ↓
Exit
```

### Data Fetching Strategy

CDS-CityFetch uses a **multi-pass approach** to reliably fetch maximum data from Wikidata:

```
┌─────────────────────────────────────────────┐
│  Pass 1: Core Data                           │
│  - City ID, name, latitude, longitude       │
│  - ~7,000 cities in 10 seconds              │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Pass 2: Country                             │
│  - Batched: 50 cities at a time             │
│  - ~99% coverage (~7-8 minutes)             │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Pass 3: Population                          │
│  - Batched: 50 cities at a time             │
│  - ~75% coverage (~7-8 minutes)             │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Pass 4: Admin Region                        │
│  - Batched: 50 cities at a time             │
│  - ~70% coverage (~7-8 minutes)             │
└─────────────────────────────────────────────┘
```

**Why this approach?**

Wikidata's SPARQL endpoint has a 60-second timeout. A single query trying to fetch all properties for 7,000+ cities will timeout. By breaking into multiple passes with small batches:

- Each query is fast and reliable
- Rate limits are respected (1 second delay between batches)
- Failed batches are retried (3 retries with exponential backoff)
- All cities are kept - no filtering

**Time:** ~20-25 minutes per language for ~7,000 cities  
**Result:** Maximum data completeness (most cities have 6-7 of 8 fields populated)

**Note:** Country code (ISO) requires additional lookups and is not currently fetched.

---

## Development

### Running locally (without Docker)

```bash
pip install -r requirements.txt
python main.py
```

### Project structure

```
.
├── main.py                  # Entry point
├── requirements.txt         # Dependencies (httpx)
├── Dockerfile             # Docker image definition
├── cityfetch/
│   ├── __init__.py              # Version info
│   ├── wikidata_service.py      # Multi-pass SPARQL fetching
│   ├── language_service.py      # Language code list
│   └── artifact_service.py      # GHCR artifact operations
├── .github/
│   └── workflows/
│       └── weekly-update.yml    # GitHub Actions workflow
└── README.md                # This file
```

---

## Artifact Storage (GHCR)

CDS-CityFetch stores city data in **GitHub Container Registry (GHCR)** as OCI artifacts.

### How It Works

**Weekly Automated Updates (GitHub Actions):**
```
1. Runs every Sunday at 2:00 AM UTC
2. Pulls existing data from GHCR for each language
3. Fetches fresh data from Wikidata
4. Merges: keeps non-null values from both (upsert)
5. Pushes merged data to GHCR as 'latest'
6. Tags previous 'latest' as 'previous' (backup)
```

**Merge Rules:**
- Existing city + new city → merge fields, keep non-null values
- Only in existing → keep it
- Only in new → add it
- If both have value → keep latest fetch

**Storage Structure:**
- `ghcr.io/filip/cityfetch-data/en:latest` - English cities
- `ghcr.io/filip/cityfetch-data/de:latest` - German cities
- `ghcr.io/filip/cityfetch-data/en:previous` - Previous version (backup)

### Pulling Data from GHCR

Public artifacts can be pulled without authentication:

```bash
# Install oras CLI (if not already installed)
# https://oras.land/cli/

# Pull English cities
oras pull ghcr.io/filip/cityfetch-data/en:latest

# Pull German cities
oras pull ghcr.io/filip/cityfetch-data/de:latest
```

Or use Docker to extract:
```bash
# Create temp container and copy files
docker run --rm -v $(pwd):/out ghcr.io/filip/cityfetch-data/en:latest \
  sh -c "cp /data/*.json /out/"
```

### Manual Artifact Update

To manually update artifacts (requires GHCR_TOKEN):

```bash
# Set your GitHub token
export GHCR_TOKEN=ghp_xxxxxxxxxxxx

# Run artifact update mode
python main.py --mode=update-artifacts
```

This will:
1. Process all 65 languages (~22 hours total)
2. Pull existing, merge with new, push to GHCR
3. Save local copies in `output/` directory

### GitHub Actions Setup

The workflow `.github/workflows/weekly-update.yml` automatically runs weekly. It requires:
- `GITHUB_TOKEN` (automatically provided by GitHub Actions)
- `packages: write` permission (configured in workflow)

No manual setup needed - just merge the workflow file to main branch.

---

## Data Source

All data is sourced from [Wikidata](https://www.wikidata.org) under [CC0 License](https://creativecommons.org/publicdomain/zero/1.0/).

---

## License

MIT License
