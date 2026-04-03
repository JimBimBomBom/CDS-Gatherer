# CDS-Gatherer

A lightweight, standalone service that fetches city data from the [Wikidata](https://www.wikidata.org) public SPARQL endpoint and writes it to a `cities.sql` file ready for import into MySQL.

Run it once to bootstrap an empty database, or leave it running as a daemon to keep your data fresh on a configurable schedule.

---

## How it works

CDS-Gatherer queries the Wikidata SPARQL endpoint for every city on Earth across one or more languages, deduplicates the results, and writes a self-contained MySQL SQL file:

```
Wikidata SPARQL
      │
      │  paginated POST requests (20 000 rows/page, up to 40 pages per language)
      ▼
Language iteration
      │
      │  circuit-breaker: aborts after N consecutive language failures
      ▼
Deduplication  (first-seen wikidata ID wins)
      │
      ▼
cities.sql  ──►  optional reload webhook  ──►  your application
```

Each city record contains:

| Field | Description |
|---|---|
| `CityId` | Wikidata Q-identifier (e.g. `Q90`) |
| `CityName` | City name in the requested language |
| `Latitude` / `Longitude` | Coordinates from Wikidata |
| `Country` | Country name |
| `CountryCode` | ISO 3166-1 alpha-2 code (e.g. `FR`) |
| `AdminRegion` | Administrative region label |
| `Population` | Population figure where available |

The output file uses `INSERT ... ON DUPLICATE KEY UPDATE` in 1 000-row batches, so it is safe to import repeatedly against an existing table.

---

## Requirements

### Python (direct)

- Python **3.12+**
- pip

```bash
pip install -r requirements.txt
```

Dependencies: `httpx==0.27.2`, `APScheduler==3.10.4`

### Docker

- Docker **24+** (or any version that supports multi-stage builds and Compose v2)

No Python installation needed on the host.

---

## Quick start

The fastest path to a `cities.sql` file — no external services required:

**Python:**
```bash
git clone https://github.com/your-org/CDS-Gatherer.git
cd CDS-Gatherer

pip install -r requirements.txt

python src/scheduler.py --once --data-dir ./output --languages en --no-webhook
# cities.sql will be at ./output/cities.sql
```

**Docker:**
```bash
docker build -t cds-gatherer .

docker run --rm \
  -v "$(pwd)/output:/app/data" \
  cds-gatherer \
  python /app/src/scheduler.py --once --data-dir /app/data --languages en --no-webhook
# cities.sql will be at ./output/cities.sql
```

A full English fetch covers roughly 60 000–100 000 cities and takes 10–20 minutes depending on Wikidata's response times.

---

## Configuration

### Environment variables

All env vars have sensible defaults so nothing is required to get started.

| Variable | Default | Description |
|---|---|---|
| `DATA_DIR` | `/app/data` | Directory where `cities.sql` is written. Overridden by `--data-dir`. |
| `DATA_GENERATION_INTERVAL_DAYS` | `30` | How often the daemon reruns the full gather cycle (days). |
| `LANG_DELAY_SECONDS` | `10` | Pause between consecutive language fetches. Keeps Wikidata happy. |
| `MAX_CONSECUTIVE_LANG_FAILURES` | `3` | Circuit-breaker threshold: abort the cycle after this many back-to-back language failures. |
| `MAX_PAGE_RETRIES` | `3` | How many times to retry a failed page before giving up on it (total attempts = retries + 1). |
| `RETRY_BASE_DELAY_SECONDS` | `30` | Wait before the first page retry. Doubles each attempt (30 → 60 → 120 → capped at 300s). |
| `CDS_APP_BASE_URL` | `http://cds-app:8080` | Base URL used to fetch the language list and send the reload webhook. Only relevant when not using `--languages` / `--no-webhook`. |
| `RELOAD_ENDPOINT` | `/internal/reload` | Webhook path appended to `CDS_APP_BASE_URL` for the reload notification. |
| `WEBHOOK_TIMEOUT` | `30` | Seconds to wait for the reload webhook response. |

Copy `DefaultGathererEnv.env` to `ProductionGathererEnv.env` and override only what you need — the Compose setup loads both files, with the production file taking precedence.

### CLI flags

CLI flags take precedence over environment variables.

| Flag | Description |
|---|---|
| `--once` | Run a single gather cycle and exit. No scheduler is started. |
| `--data-dir <path>` | Write `cities.sql` to `<path>`. Overrides `DATA_DIR`. |
| `--languages <codes>` | Comma-separated language codes to fetch (e.g. `en,de,fr`). Skips any external language-list fetch entirely. |
| `--no-webhook` | Do not send a reload notification after writing the SQL file. |

---

## Running modes

### One-shot (standalone)

Run once, write the file, exit. No scheduler, no external dependencies. Ideal for seeding an empty database on first deploy or running from a CI job.

```bash
# Fetch English and German cities, write to /tmp/cities, no webhook
python src/scheduler.py \
  --once \
  --data-dir /tmp/cities \
  --languages en,de \
  --no-webhook
```

### Daemon (scheduled)

Runs an immediate gather cycle on startup, then repeats every `DATA_GENERATION_INTERVAL_DAYS` days. This is the default mode when no `--once` flag is provided.

```bash
# Daemon mode — runs now, then every 7 days
DATA_GENERATION_INTERVAL_DAYS=7 \
DATA_DIR=/var/data/cities \
python src/scheduler.py --languages en,cs,sk,de,fr --no-webhook
```

### Daemon with reload webhook

If you have an application that accepts a reload notification, point the gatherer at it. The webhook receives a JSON `POST` with the path to the newly written SQL file:

```jsonc
// POST http://your-app:8080/internal/reload
{ "sqlFilePath": "/app/data/cities.sql" }
```

```bash
CDS_APP_BASE_URL=http://your-app:8080 \
RELOAD_ENDPOINT=/internal/reload \
DATA_DIR=/app/data \
python src/scheduler.py
```

---

## Docker

### Build

```bash
docker build -t cds-gatherer .
```

### Run — one-shot, output to host folder

```bash
docker run --rm \
  -v "$(pwd)/output:/app/data" \
  cds-gatherer \
  python /app/src/scheduler.py \
    --once \
    --data-dir /app/data \
    --languages en,de,fr \
    --no-webhook
```

### Run — daemon, env file, named volume

```bash
docker run -d \
  --name cds-gatherer \
  --env-file DefaultGathererEnv.env \
  -v cds_data:/app/data \
  --restart unless-stopped \
  cds-gatherer
```

### Pass CLI flags via Docker

Any arguments after the image name are forwarded to `scheduler.py`:

```bash
docker run --rm \
  -v "$(pwd)/output:/app/data" \
  cds-gatherer \
  python /app/src/scheduler.py --once --data-dir /app/data --no-webhook
```

---

## Docker Compose

`compose.yaml` is included for running the gatherer as part of a larger stack. It expects an external Docker network called `cdsNetwork` and a named volume `cds_data` that is shared with your application container.

```yaml
# compose.yaml (excerpt)
services:
  cds-gatherer:
    build: .
    env_file:
      - DefaultGathererEnv.env
      - path: ProductionGathererEnv.env
        required: false
    volumes:
      - cds_data:/app/data
    networks:
      - cdsNetwork
    restart: unless-stopped
```

Start it alongside your stack:

```bash
docker compose up -d
```

To pass CLI flags in Compose, override `command`:

```yaml
command: ["python", "/app/src/scheduler.py", "--once", "--no-webhook"]
```

---

## Output format

`cities.sql` is a self-contained MySQL script. It targets this table schema:

```sql
CREATE TABLE cities (
    CityId      VARCHAR(20)    PRIMARY KEY,   -- Wikidata Q-id, e.g. 'Q90'
    CityName    VARCHAR(255)   NOT NULL,
    Latitude    DECIMAL(10,8)  NOT NULL,
    Longitude   DECIMAL(11,8)  NOT NULL,
    CountryCode VARCHAR(2)     NULL,
    Country     VARCHAR(100)   NULL,
    AdminRegion VARCHAR(100)   NULL,
    Population  INT            NULL
);
```

The file uses batched upserts so it is safe to re-import against a populated table:

```sql
-- Auto-generated SQL file from Wikidata
-- Generated at: 2026-04-03 12:00:00 UTC
-- Total records: 87432

INSERT INTO cities (CityId, CityName, Latitude, Longitude, CountryCode, Country, AdminRegion, Population) VALUES
    ('Q90',   'Paris',  48.85341000,  2.34880000, 'FR', 'France',  'Île-de-France', 2161000),
    ('Q1726', 'Munich', 48.13743000, 11.57549000, 'DE', 'Germany', 'Bavaria',       1487708),
    ...
ON DUPLICATE KEY UPDATE
    CityName    = VALUES(CityName),
    Latitude    = VALUES(Latitude),
    ...
    Population  = VALUES(Population);
```

---

## Troubleshooting

**Wikidata returns HTTP 429 (Too Many Requests) or 504 (Gateway Timeout)**

Transient errors (429, 500, 502, 503, 504) are automatically retried up to `MAX_PAGE_RETRIES` times with exponential backoff (30s → 60s → 120s, capped at 300s). A `Retry-After` header on a 429 response is respected.

If you are still hitting rate limits consistently, increase the between-language delay:

```bash
LANG_DELAY_SECONDS=30 python src/scheduler.py ...
```

You can also reduce the number of languages fetched in one run with `--languages`.

---

**The container can't write the output file**

If you mount a host directory, ensure the host folder is writable. The container runs as a non-root user (`gatherer`). A quick fix:

```bash
mkdir -p ./output && chmod 777 ./output
docker run --rm -v "$(pwd)/output:/app/data" cds-gatherer ...
```

---

**No cities are returned for a language**

Some less common language codes return sparse results from Wikidata. Use `--languages` to restrict to languages you know are well-populated (e.g. `en,de,fr,es,pt,it,nl,pl,ru,ja,zh`).

---

**Language list fetch fails (no `--languages` flag set)**

Without `--languages`, the gatherer tries to fetch a language list from the URL set in `CDS_APP_BASE_URL`. If that host is unreachable, it automatically falls back to a built-in list of 20 languages:

```
en, cs, sk, de, fr, es, it, pt, pl, nl, ru, ja, zh, ar, ko, sv, tr, fi, hu, no
```

No action is needed — this is expected behaviour when running standalone.
