"""
scheduler.py
------------
Entry point for the CDS-Gatherer service.

On startup it runs one gather cycle immediately, then repeats on the
configured interval (default: every 30 days).

Can also be invoked in "run-once" mode (--once) to fetch data a single time
and exit — useful when bootstrapping an empty database without a running
cds-app instance.

Environment variables
~~~~~~~~~~~~~~~~~~~~~
DATA_GENERATION_INTERVAL_DAYS   How often to run (default: 30)
MAX_CONSECUTIVE_LANG_FAILURES   Circuit-breaker threshold (default: 3)
LANG_DELAY_SECONDS              Pause between languages (default: 10)
CDS_APP_BASE_URL                Base URL of cds-app for languages + reload
RELOAD_ENDPOINT                 Reload webhook path on cds-app
DATA_DIR                        Directory for the SQL file (default: /app/data)

CLI flags (take precedence over environment variables)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--data-dir <path>       Directory where cities.sql will be written.
                        Overrides DATA_DIR env var.
--once                  Run a single gather cycle and exit; no scheduler.
--languages en,de,fr    Comma-separated language codes to fetch.
                        Skips the cds-app /languages call entirely.
--no-webhook            Do not POST a reload notification to cds-app after
                        the SQL file is written.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler

import language_service
import sql_generator
import webhook
from wikidata_service import SparqlCityInfo, fetch_cities

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
logger = logging.getLogger("scheduler")

# ---------------------------------------------------------------------------
# Config (env vars; CLI flags parsed in main() override these)
# ---------------------------------------------------------------------------

INTERVAL_DAYS = max(1, int(os.environ.get("DATA_GENERATION_INTERVAL_DAYS", "30")))
MAX_CONSECUTIVE_LANG_FAILURES = max(1, int(os.environ.get("MAX_CONSECUTIVE_LANG_FAILURES", "3")))
LANG_DELAY_SECONDS = max(0, int(os.environ.get("LANG_DELAY_SECONDS", "10")))

# ---------------------------------------------------------------------------
# Core gather job
# ---------------------------------------------------------------------------


def run_gather_cycle(
    data_dir: str | None = None,
    language_override: str | None = None,
    no_webhook: bool = False,
) -> None:
    """
    Full gather cycle:
      1. Resolve language list — from CLI override, cds-app /languages, or fallback
      2. For each language: fetch cities from Wikidata SPARQL
      3. Deduplicate and generate cities.sql in *data_dir*
      4. POST reload notification to cds-app (unless *no_webhook* is True)
    """
    started_at = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Gather cycle started at %s", started_at.isoformat())
    logger.info("=" * 60)

    # Step 1 – resolve language list
    languages = language_service.fetch_language_codes(override=language_override)
    logger.info("Processing %d language(s): %s", len(languages), languages)

    # Step 2 – fetch from Wikidata
    all_cities: list[SparqlCityInfo] = []
    consecutive_failures = 0

    for idx, lang in enumerate(languages):
        try:
            logger.info("--- Language %d/%d: %s ---", idx + 1, len(languages), lang)
            cities = fetch_cities(lang)

            if cities:
                all_cities.extend(cities)
                logger.info("Fetched %d cities for '%s'. Running total: %d", len(cities), lang, len(all_cities))
            else:
                logger.warning("No cities returned for language '%s'.", lang)

            consecutive_failures = 0  # reset on success

        except Exception as exc:  # noqa: BLE001
            consecutive_failures += 1
            logger.error(
                "Failed to fetch cities for language '%s': %s  (consecutive failures: %d/%d)",
                lang, exc, consecutive_failures, MAX_CONSECUTIVE_LANG_FAILURES,
            )
            if consecutive_failures >= MAX_CONSECUTIVE_LANG_FAILURES:
                logger.error(
                    "Circuit breaker triggered after %d consecutive language failures. "
                    "Aborting fetch; will proceed with %d cities collected so far.",
                    consecutive_failures, len(all_cities),
                )
                break

        # Polite delay between languages (skip after last one)
        if idx < len(languages) - 1 and LANG_DELAY_SECONDS > 0:
            logger.info("Waiting %ds before next language...", LANG_DELAY_SECONDS)
            time.sleep(LANG_DELAY_SECONDS)

    if not all_cities:
        logger.warning("No cities fetched from Wikidata in this cycle. Skipping SQL generation.")
        return

    # Step 3 – generate SQL file
    try:
        sql_path = sql_generator.generate_sql_file(all_cities, data_dir=data_dir)
    except Exception as exc:  # noqa: BLE001
        logger.error("SQL generation failed: %s", exc)
        return

    # Step 4 – notify cds-app to reload
    success = webhook.notify_reload(sql_path, enabled=not no_webhook)
    if not success:
        logger.warning(
            "Reload notification failed. The SQL file is still available at %s; "
            "the app may reload it on its next startup or manual trigger.",
            sql_path,
        )

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info("=" * 60)
    logger.info("Gather cycle complete in %.0fs.", elapsed)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cds-gatherer",
        description=(
            "Fetch city data from Wikidata and write a cities.sql file. "
            "Runs on a scheduled interval by default, or once with --once."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Daemon mode (default): run every 30 days
  python scheduler.py

  # Run once, write to a custom folder, no cds-app needed
  python scheduler.py --once --data-dir /tmp/cities --no-webhook

  # Run once for specific languages only
  python scheduler.py --once --languages en,de,fr --data-dir ./output

  # Override interval to 7 days (can also set DATA_GENERATION_INTERVAL_DAYS)
  python scheduler.py --data-dir /data/cities
        """,
    )
    parser.add_argument(
        "--data-dir",
        metavar="PATH",
        default=None,
        help=(
            "Directory where cities.sql will be written. "
            "Overrides the DATA_DIR environment variable."
        ),
    )
    parser.add_argument(
        "--once",
        action="store_true",
        default=False,
        help="Run a single gather cycle and exit. No scheduler is started.",
    )
    parser.add_argument(
        "--languages",
        metavar="CODES",
        default=None,
        help=(
            "Comma-separated language codes to fetch (e.g. 'en,de,fr'). "
            "Skips the cds-app /languages endpoint entirely."
        ),
    )
    parser.add_argument(
        "--no-webhook",
        action="store_true",
        default=False,
        help="Skip the reload POST notification to cds-app after writing the SQL file.",
    )
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    args = _build_arg_parser().parse_args()

    logger.info(
        "CDS-Gatherer starting. Interval: %d day(s). Max consecutive failures: %d.",
        INTERVAL_DAYS, MAX_CONSECUTIVE_LANG_FAILURES,
    )

    if args.data_dir:
        logger.info("Output directory (CLI): %s", args.data_dir)
    if args.languages:
        logger.info("Language override (CLI): %s", args.languages)
    if args.no_webhook:
        logger.info("Webhook notifications disabled (--no-webhook).")

    cycle_kwargs: dict = {
        "data_dir": args.data_dir,
        "language_override": args.languages,
        "no_webhook": args.no_webhook,
    }

    if args.once:
        logger.info("Running in one-shot mode (--once).")
        run_gather_cycle(**cycle_kwargs)
        logger.info("One-shot complete. Exiting.")
        sys.exit(0)

    # Daemon mode: run immediately on startup, then on the configured interval
    run_gather_cycle(**cycle_kwargs)

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_gather_cycle,
        trigger="interval",
        days=INTERVAL_DAYS,
        kwargs=cycle_kwargs,
        id="gather_cycle",
        name="Wikidata city gather",
        misfire_grace_time=3600,  # allow up to 1h late if the container was down
    )

    logger.info("Scheduler started. Next run in %d day(s).", INTERVAL_DAYS)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("CDS-Gatherer shutting down.")


if __name__ == "__main__":
    main()
