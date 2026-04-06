# CDS-CityFetch

A lightweight, standalone CLI tool that fetches city data from [Wikidata](https://www.wikidata.org) and exports it to SQL (MySQL), JSON, or CSV format.

No Python knowledge required. No installation needed. Just download and run.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Output Formats](#output-formats)
- [Scheduled Fetching](#scheduled-fetching)
- [Building from Source](#building-from-source)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Features

- **Docker-ready** – Pre-built image with automatic scheduling support
- **One binary, zero dependencies** – Single executable file (~15-20MB) with everything included
- **Multiple output formats** – SQL (MySQL), JSON, CSV
- **Multi-language support** – Fetch city names in any language supported by Wikidata
- **Smart deduplication** – Handles overlapping data across languages
- **Automatic retries** – Handles rate limits and transient failures
- **Webhook notifications** – Notify your app when data updates
- **Progress tracking** – Visual progress bars for long operations
- **Cross-platform** – Linux, Windows, macOS
- **Flexible scheduling** – Docker built-in scheduler or cron integration

---

## Installation

Choose the method that works best for you:

1. **Docker (Recommended for servers)** – Pre-built image, easiest setup, automatic scheduling
2. **Pre-built Binary** – Single executable, no dependencies
3. **Package Managers** – System-native installation
4. **Python (from source)** – For developers

### Option 1: Docker (Recommended for Production)

The easiest way to run CDS-CityFetch with automatic scheduling.

#### Quick Start with Docker Compose

```bash
# Download the compose file
curl -O https://raw.githubusercontent.com/filip/cds-cityfetch/main/docker-compose.yml

# Create output directory
mkdir -p output

# Edit docker-compose.yml to set your languages (default: en)
# Then start the container
docker-compose up -d

# Check logs
docker-compose logs -f

# View output
ls output/
```

#### Docker Run (One-shot)

```bash
# Single execution - fetch once and exit
docker run --rm \
  -e LANGUAGES=en,de,fr \
  -e SCHEDULE=0DAYS \
  -v $(pwd)/output:/data \
  ghcr.io/filip/cds-cityfetch:latest

# View results
ls output/
```

#### Docker Run (Scheduled)

```bash
# Weekly automatic updates
docker run -d \
  --name cityfetch \
  -e LANGUAGES=en,de,fr \
  -e SCHEDULE=7DAYS \
  -e VERBOSE=true \
  -v $(pwd)/output:/data \
  --restart unless-stopped \
  ghcr.io/filip/cds-cityfetch:latest

# Check status
docker logs cityfetch -f
```

#### Docker Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LANGUAGES` | `en` | **Required.** Comma-separated language codes |
| `OUTPUT_FORMAT` | `sql` | Output format: `sql`, `json`, or `csv` |
| `OUTPUT_DIR` | `/data` | Output directory inside container |
| `SCHEDULE` | `7DAYS` | Schedule: `0DAYS`=once, `7DAYS`=weekly, `30DAYS`=monthly |
| `WEBHOOK_URL` | (none) | URL to notify after successful fetch |
| `WEBHOOK_SECRET` | (none) | Secret token for webhook auth |
| `VERBOSE` | `false` | Enable detailed logging |

### Option 2: Download Pre-built Binary

#### Linux

```bash
# Download the latest release
curl -L https://github.com/filip/cds-cityfetch/releases/latest/download/cityfetch-linux -o cityfetch

# Make it executable
chmod +x cityfetch

# Move to a directory in your PATH
sudo mv cityfetch /usr/local/bin/

# Verify installation
cityfetch version
```

#### Windows

**Using PowerShell (Run as Administrator):**

```powershell
# Download the binary
Invoke-WebRequest -Uri "https://github.com/filip/cds-cityfetch/releases/latest/download/cityfetch-windows.exe" -OutFile "cityfetch.exe"

# Move to a directory in your PATH
# Option 1: Move to System32 (requires admin)
Move-Item -Path ".\cityfetch.exe" -Destination "C:\Windows\System32\cityfetch.exe"

# Option 2: Create custom folder
New-Item -ItemType Directory -Force -Path "C:\Tools"
Move-Item -Path ".\cityfetch.exe" -Destination "C:\Tools\cityfetch.exe"
# Add C:\Tools to your PATH environment variable

# Verify installation
cityfetch version
```

**Manual Download:**
1. Download `cityfetch-windows.exe` from the [releases page](https://github.com/filip/cds-cityfetch/releases)
2. Rename to `cityfetch.exe`
3. Move to a folder in your PATH

#### macOS

```bash
# Download the latest release
curl -L https://github.com/filip/cds-cityfetch/releases/latest/download/cityfetch-macos -o cityfetch

# Make it executable
chmod +x cityfetch

# Move to a directory in your PATH
sudo mv cityfetch /usr/local/bin/

# Verify installation
cityfetch version
```

### Option 2: Package Managers

#### Homebrew (macOS/Linux)

```bash
brew tap filip/cds-cityfetch
brew install cityfetch
```

#### Scoop (Windows)

```powershell
scoop bucket add cds-cityfetch https://github.com/filip/cds-cityfetch-bucket
scoop install cityfetch
```

#### APT (Debian/Ubuntu)

```bash
# Add the repository
curl -s https://filip.github.io/cds-cityfetch/apt/gpg.key | sudo apt-key add -
echo "deb https://filip.github.io/cds-cityfetch/apt stable main" | sudo tee /etc/apt/sources.list.d/cds-cityfetch.list

# Install
sudo apt update
sudo apt install cityfetch
```

### Option 3: Run with Python (Developers)

If you have Python 3.12+ installed:

```bash
# Clone the repository
git clone https://github.com/filip/cds-cityfetch.git
cd cds-cityfetch

# Install dependencies
pip install -r requirements.txt

# Run directly
python -m cityfetch --help
```

---

## Quick Start

### Docker (Fastest Setup)

```bash
# One command to start automatic weekly updates
docker run -d \
  --name cityfetch \
  -e LANGUAGES=en \
  -e SCHEDULE=7DAYS \
  -v $(pwd)/output:/data \
  --restart unless-stopped \
  ghcr.io/filip/cds-cityfetch:latest

# Check logs
docker logs cityfetch -f
```

### CLI Tool

```bash
# Fetch English cities
cityfetch fetch -l en

# Multiple languages
cityfetch fetch -l en,de,fr

# Export to JSON
cityfetch fetch -l en -f json

# With webhook notification
cityfetch fetch -l en --webhook-url http://myapp:8080/reload
```

---

## Usage

### Commands

```
cityfetch [COMMAND] [OPTIONS]
```

**Available Commands:**

| Command | Description |
|---------|-------------|
| `fetch` | Fetch city data from Wikidata (main command) |
| `cron` | Show cron setup instructions |
| `version` | Show version information |

### Fetch Command Options

```
cityfetch fetch [OPTIONS]
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--languages` | `-l` | **Required** | Comma-separated language codes (e.g., `en,de,fr`) |
| `--output` | `-o` | `cities.sql` | Output filename (auto-extended based on format) |
| `--dir` | | `.` | Output directory |
| `--format` | `-f` | `sql` | Output format: `sql`, `json`, or `csv` |
| `--data-dir` | `-d` | Same as `--dir` | Working directory for temporary files |
| `--batch-size` | | `1000` | Rows per SQL batch (SQL format only) |
| `--max-pages` | | `40` | Maximum pages to fetch per language |
| `--page-size` | | `500` | Cities per Wikidata request |
| `--verbose` | `-v` | | Show detailed progress information |
| `--dry-run` | | | Simulate without writing files |
| `--webhook-url` | | | URL to POST notification after successful fetch |
| `--webhook-secret` | | | Secret token for webhook authentication |

### Examples

**Basic SQL export:**
```bash
cityfetch fetch -l en
```

**Multiple languages with custom output:**
```bash
cityfetch fetch -l en,de,fr,es,it -o ./output/world-cities.sql -v
```

**JSON export with progress bar:**
```bash
cityfetch fetch -l en -f json -o cities.json --verbose
```

**CSV export:**
```bash
cityfetch fetch -l de -f csv -o german-cities.csv
```

**Dry run (test without downloading):**
```bash
cityfetch fetch -l en --dry-run -v
```

**Custom batch size for large datasets:**
```bash
cityfetch fetch -l en --batch-size 5000 -o cities-large.sql
```

---

## Output Formats

### SQL (Default)

MySQL-compatible SQL file with `INSERT ... ON DUPLICATE KEY UPDATE` statements.

**Schema:**
```sql
CREATE TABLE cities (
    CityId      VARCHAR(20)    PRIMARY KEY,
    CityName    VARCHAR(255)   NOT NULL,
    Latitude    DECIMAL(10,8)  NOT NULL,
    Longitude   DECIMAL(11,8)  NOT NULL,
    CountryCode VARCHAR(2)     NULL,
    Country     VARCHAR(100)   NULL,
    AdminRegion VARCHAR(100)   NULL,
    Population  INT            NULL
);
```

**Usage:**
```bash
mysql -u username -p database < cities.sql
```

### JSON

Structured JSON format with metadata and cities array.

**Structure:**
```json
{
  "metadata": {
    "generated_at": "2026-04-05 21:30:00 UTC",
    "tool": "CDS-CityFetch",
    "tool_version": "1.0.0",
    "source": "Wikidata",
    "total_records": 87432
  },
  "cities": [
    {
      "city_id": "Q90",
      "city_name": "Paris",
      "language": "en",
      "latitude": 48.85341000,
      "longitude": 2.34880000,
      "country": "France",
      "country_code": "FR",
      "admin_region": "Île-de-France",
      "population": 2161000
    }
  ]
}
```

### CSV

Comma-separated values with headers.

**Columns:**
- `city_id` – Wikidata Q-identifier
- `city_name` – City name
- `language` – Language code
- `latitude` – Geographic latitude
- `longitude` – Geographic longitude
- `country` – Country name
- `country_code` – ISO 3166-1 alpha-2 code
- `admin_region` – Administrative region
- `population` – Population count

---

## OCI Registry (GitHub Container Registry)

CDS-CityFetch can push and pull data artifacts to/from OCI-compliant registries like GitHub Container Registry (GHCR). This enables:

- **Versioned datasets** – Keep historical snapshots of city data
- **Team sharing** – Share datasets across your organization
- **CI/CD integration** – Fetch specific data versions in your pipelines
- **Rollback capability** – Use older data versions if needed

### Setup

Set your GitHub token as an environment variable:

```bash
export GITHUB_TOKEN=ghp_xxxxxxxxxxxx  # Your GitHub Personal Access Token
```

Required token scopes: `read:packages`, `write:packages`, `delete:packages`

### Push Data to Registry

After fetching, push the result to GHCR:

```bash
# Fetch and push English data
cityfetch fetch -l en --push --registry ghcr.io/filip/cds-cityfetch

# Creates two tags:
# - ghcr.io/filip/cds-cityfetch:en-20240412-143022 (timestamped)
# - ghcr.io/filip/cds-cityfetch:en-latest (rolling)
```

### Pull Data from Registry

Download a specific version:

```bash
# Pull latest English data
cityfetch pull ghcr.io/filip/cds-cityfetch:en-latest --dir ./data

# Pull specific timestamp
cityfetch pull ghcr.io/filip/cds-cityfetch:en-20240412-143022 --dir ./data

# Output:
# ✓ Successfully downloaded cities.sql
#   Location: ./data/cities.sql
#   Records: 87432
#   Languages: en
#   Originally fetched: 2026-04-12T14:30:22Z
#   Metadata: ./data/cities.sql.meta
```

### List Available Artifacts

See what datasets are available:

```bash
cityfetch list ghcr.io/filip/cds-cityfetch

# Output:
# Found 6 artifacts:
# 
# de:
#   de-latest ← current
#   de-20240412-143022
#   de-20240405-120000
# 
# en:
#   en-latest ← current
#   en-20240412-143022
#   en-20240405-120000
```

### Client-Side Merge Workflow

Merge existing registry data with fresh Wikidata fetch:

```bash
# Step 1: Pull existing data
cityfetch pull ghcr.io/filip/cds-cityfetch:en-latest --dir ./temp

# Step 2: Fetch fresh data and merge
cityfetch fetch -l en \
  --merge-with ./temp/cities.sql \
  --push \
  --registry ghcr.io/filip/cds-cityfetch

# Result: New version with merged data
```

**Merge strategy:** Upsert (new data overwrites existing on duplicate CityId)

### Metadata Files

When pulling, a metadata sidecar file is created:

```bash
cat cities.sql.meta
```

```json
{
  "registry": "ghcr.io/filip/cds-cityfetch",
  "tag": "en-20240412-143022",
  "fetched_at": "2026-04-12T14:30:22Z",
  "record_count": 87432,
  "languages": ["en"],
  "format": "sql"
}
```

### Docker with Registry

```yaml
services:
  cityfetch:
    image: ghcr.io/filip/cds-cityfetch:latest
    environment:
      - LANGUAGES=en
      - SCHEDULE=7DAYS
      - PUSH_REGISTRY=ghcr.io/filip/cds-cityfetch  # Auto-push after fetch
    volumes:
      - ./output:/data
    restart: unless-stopped
```

---

## Scheduled Fetching

### Docker (Easiest Method)

The Docker image has built-in scheduling support. Set `SCHEDULE` environment variable:

**Schedule Options:**
- `0DAYS` – Run once and exit
- `1DAYS` – Daily
- `7DAYS` – Weekly (default)
- `30DAYS` – Monthly
- Any number: `14DAYS`, `90DAYS`, etc.

**Weekly automatic updates:**

```bash
docker run -d \
  --name cityfetch \
  -e LANGUAGES=en,de,fr \
  -e SCHEDULE=7DAYS \
  -e VERBOSE=true \
  -v $(pwd)/output:/data \
  --restart unless-stopped \
  ghcr.io/filip/cds-cityfetch:latest
```

**Daily updates:**

```bash
docker run -d \
  --name cityfetch \
  -e LANGUAGES=en \
  -e SCHEDULE=1DAYS \
  -v $(pwd)/output:/data \
  --restart unless-stopped \
  ghcr.io/filip/cds-cityfetch:latest
```

**With Docker Compose:**

Edit `docker-compose.yml` to uncomment the desired schedule:

```yaml
services:
  cityfetch:
    image: ghcr.io/filip/cds-cityfetch:latest
    environment:
      - LANGUAGES=en,de,fr
      - SCHEDULE=7DAYS  # Change this value
    volumes:
      - ./output:/data
    restart: unless-stopped
```

Then start:
```bash
docker-compose up -d
```

### Native Binary with Cron (Linux/macOS)

To automatically fetch city data on a schedule, use `cron` (Linux/macOS) or `Task Scheduler` (Windows).

#### Show Cron Setup Instructions

```bash
cityfetch cron -l en
```

This displays the crontab line needed for scheduled fetching.

### Manual Cron Setup (Linux/macOS)

**Weekly fetch (Sundays at 2 AM):**

```bash
# Edit your crontab
crontab -e

# Add this line:
0 2 * * 0 /usr/local/bin/cityfetch fetch -l en -o /path/to/cities.sql
```

**Daily fetch (every day at 2 AM):**

```bash
0 2 * * * /usr/local/bin/cityfetch fetch -l en,de,fr -o /path/to/cities.sql
```

**Multiple languages with JSON output:**

```bash
0 3 * * 1 /usr/local/bin/cityfetch fetch -l en,de,fr,es,it -f json -o /path/to/cities.json
```

### Windows Task Scheduler

**Using PowerShell:**

```powershell
# Create a daily task at 2 AM
$Action = New-ScheduledTaskAction -Execute "cityfetch" -Argument "fetch -l en -o C:\Data\cities.sql"
$Trigger = New-ScheduledTaskTrigger -Daily -At 2am
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERNAME" -LogonType ServiceAccount
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName "CityFetch-Daily" -Action $Action -Trigger $Trigger -Principal $Principal -Settings $Settings

# Verify the task was created
Get-ScheduledTask -TaskName "CityFetch-Daily"
```

**Manual Setup:**
1. Open Task Scheduler (`taskschd.msc`)
2. Create Basic Task
3. Set trigger (Daily/Weekly)
4. Set action: Start a program
   - Program: `cityfetch` (or full path)
   - Arguments: `fetch -l en -o C:\Data\cities.sql`

---

## Webhook Notifications

CDS-CityFetch can notify your application when a fetch completes successfully. This is useful for triggering reloads in databases, search indexes, or applications.

### CLI Usage

```bash
# Basic webhook
cityfetch fetch -l en --webhook-url http://myapp:8080/reload

# With authentication
cityfetch fetch -l en \
  --webhook-url http://myapp:8080/api/data-updated \
  --webhook-secret my-secret-token
```

### Docker Usage

```bash
docker run -d \
  --name cityfetch \
  -e LANGUAGES=en \
  -e SCHEDULE=7DAYS \
  -e WEBHOOK_URL=http://myapp:8080/reload \
  -e WEBHOOK_SECRET=my-secret-token \
  -v $(pwd)/output:/data \
  --restart unless-stopped \
  ghcr.io/filip/cds-cityfetch:latest
```

### Webhook Payload

When a fetch completes, a POST request is sent to your webhook URL with this JSON payload:

```json
{
  "event": "fetch_complete",
  "data": {
    "file_path": "/data/cities.sql",
    "absolute_path": "/data/cities.sql",
    "format": "sql",
    "languages": ["en"],
    "record_count": 87432,
    "timestamp": "2026-04-05T21:30:00Z",
    "success": true
  }
}
```

### Webhook Headers

| Header | Description |
|--------|-------------|
| `Content-Type` | `application/json` |
| `X-Webhook-Secret` | Your secret token (if `--webhook-secret` is provided) |

### Example Webhook Handler (Node.js/Express)

```javascript
app.post('/reload', (req, res) => {
  const secret = req.headers['x-webhook-secret'];
  
  // Verify secret
  if (secret !== process.env.WEBHOOK_SECRET) {
    return res.status(401).send('Unauthorized');
  }
  
  const { file_path, record_count } = req.body.data;
  console.log(`Data updated: ${record_count} cities at ${file_path}`);
  
  // Reload your database, clear cache, etc.
  reloadDatabase(file_path);
  
  res.json({ status: 'ok' });
});
```

---

## Building from Source

### Prerequisites

- Python 3.12+
- pip

### Build Steps

```bash
# Clone the repository
git clone https://github.com/filip/cds-cityfetch.git
cd cds-cityfetch

# Install dependencies
pip install -r requirements.txt

# Run tests (if available)
python -m pytest

# Build standalone binary with PyInstaller
pip install pyinstaller
python build.py

# Binaries will be in dist/
ls dist/
# cityfetch-linux, cityfetch-windows.exe, cityfetch-macos
```

### Build for Specific Platform

**Linux:**
```bash
pyinstaller --onefile --name cityfetch-linux cityfetch/__main__.py
```

**Windows:**
```powershell
pyinstaller --onefile --name cityfetch-windows.exe cityfetch\__main__.py
```

**macOS:**
```bash
pyinstaller --onefile --name cityfetch-macos cityfetch/__main__.py
```

---

## Environment Variables

For advanced configuration, you can set these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_PAGE_RETRIES` | `3` | Retry attempts for failed Wikidata pages |
| `RETRY_BASE_DELAY_SECONDS` | `30` | Initial delay between retries (doubles each attempt) |

**Example:**
```bash
MAX_PAGE_RETRIES=5 cityfetch fetch -l en
```

---

## Troubleshooting

### "command not found" / "cityfetch is not recognized"

**Linux/macOS:**
- Ensure the binary is in your PATH: `echo $PATH`
- Check if file is executable: `ls -la /usr/local/bin/cityfetch`
- Make it executable: `chmod +x /usr/local/bin/cityfetch`

**Windows:**
- Ensure the directory containing `cityfetch.exe` is in your PATH
- Check Environment Variables: `System Properties → Advanced → Environment Variables → Path`
- Try using the full path: `C:\Tools\cityfetch.exe fetch -l en`

### "Permission denied" (Linux/macOS)

```bash
# Fix permissions
chmod +x cityfetch

# Or move with sudo
sudo mv cityfetch /usr/local/bin/
sudo chmod +x /usr/local/bin/cityfetch
```

### Wikidata Rate Limiting (HTTP 429)

If you hit rate limits:

1. **Reduce languages per run:**
   ```bash
   cityfetch fetch -l en  # Instead of 10+ languages
   ```

2. **Set longer delays via environment:**
   ```bash
   RETRY_BASE_DELAY_SECONDS=60 cityfetch fetch -l en
   ```

3. **Fetch during off-peak hours** (scheduled via cron)

### No cities returned

- Verify language code is valid (e.g., `en`, not `english`)
- Some niche languages have sparse data in Wikidata
- Try using a more common language like `en` (English) or `de` (German)

### Output file not created

- Ensure output directory exists: `mkdir -p ./output`
- Check write permissions: `ls -la ./output`
- Use absolute paths: `cityfetch fetch -l en -o /home/user/data/cities.sql`

### "SSL certificate verify failed"

Rare SSL issue (usually on older systems):

```bash
# Update certificates (Ubuntu/Debian)
sudo apt-get update && sudo apt-get install ca-certificates

# Or temporarily disable (not recommended for production)
export PYTHONHTTPSVERIFY=0
cityfetch fetch -l en
```

---

## Language Codes Reference

Common Wikidata language codes:

| Code | Language | Code | Language |
|------|----------|------|----------|
| `en` | English | `de` | German |
| `fr` | French | `es` | Spanish |
| `it` | Italian | `pt` | Portuguese |
| `nl` | Dutch | `pl` | Polish |
| `ru` | Russian | `ja` | Japanese |
| `zh` | Chinese | `ar` | Arabic |
| `ko` | Korean | `sv` | Swedish |
| `tr` | Turkish | `fi` | Finnish |
| `hu` | Hungarian | `no` | Norwegian |
| `cs` | Czech | `sk` | Slovak |

Use any valid [BCP 47](https://tools.ietf.org/html/bcp47) language tag.

---

## License

MIT License – See [LICENSE](LICENSE) file for details.

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on [GitHub](https://github.com/filip/cds-cityfetch).

---

## Support

- **Issues:** [GitHub Issues](https://github.com/filip/cds-cityfetch/issues)
- **Discussions:** [GitHub Discussions](https://github.com/filip/cds-cityfetch/discussions)
- **Email:** filip.dvorak13@gmail.com

---

**Made with ❤️ for the open data community.**

Data sourced from [Wikidata](https://www.wikidata.org) under [CC0 License](https://creativecommons.org/publicdomain/zero/1.0/).
