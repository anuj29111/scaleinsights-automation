#!/usr/bin/env python3
"""
ScaleInsights Daily Rankings Pull

Main orchestrator: login → download → parse → upsert into Supabase.

Usage:
    # All countries, 30-day window
    python scripts/pull_rankings.py

    # Single country, 7-day window
    python scripts/pull_rankings.py --country US --days 7

    # Download only (skip DB insert)
    python scripts/pull_rankings.py --country US --days 7 --dry-run
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils.scraper import ScaleInsightsScraper
from scripts.utils.parser import parse_excel
from scripts.utils.db import (
    MARKETPLACE_UUIDS,
    SI_COUNTRY_CODES,
    COUNTRY_MIN_SIZE,
    get_supabase_client,
    create_import_record,
    update_import_record,
    upsert_keywords,
    fetch_keyword_ids,
    upsert_ranks,
    wait_for_db_sync,
)
from scripts.utils.alerting import get_alert_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# All supported countries (in processing order)
ALL_COUNTRIES = ["US", "CA", "UK", "DE", "FR", "AU"]

# Delay between country downloads (seconds) — avoid rate limiting
INTER_COUNTRY_DELAY = 5


def process_country(
    scraper: ScaleInsightsScraper,
    country: str,
    from_date: str,
    to_date: str,
    dry_run: bool = False,
) -> dict:
    """
    Process a single country: download, parse, upsert.

    Returns:
        Result dict with status, counts, error info
    """
    result = {
        "country": country,
        "status": "failed",
        "keywords": 0,
        "ranks": 0,
        "error": None,
    }

    marketplace_id = MARKETPLACE_UUIDS[country]
    si_code = SI_COUNTRY_CODES[country]
    min_size = COUNTRY_MIN_SIZE[country]

    try:
        # --- Download ---
        logger.info(f"--- {country} ---")
        file_bytes = scraper.download_rankings(si_code, from_date, to_date)

        # Validate file size
        if len(file_bytes) < min_size:
            raise ValueError(
                f"File too small: {len(file_bytes):,} bytes "
                f"(minimum: {min_size:,} bytes). "
                f"Likely an error page, not an Excel file."
            )

        logger.info(f"{country}: Downloaded {len(file_bytes):,} bytes")

        if dry_run:
            logger.info(f"{country}: Dry run — skipping DB insert")
            result["status"] = "completed"
            return result

        # --- Create import record ---
        filename = f"KeywordRanking_{country}_{from_date}_{to_date}.xlsx"
        import_id = create_import_record(
            marketplace_id=marketplace_id,
            date_min=from_date,
            date_max=to_date,
            filename=filename,
        )

        try:
            # --- Parse ---
            keyword_records, merged_ranks, sorted_dates, stats = parse_excel(
                file_bytes, marketplace_id, import_id
            )

            logger.info(
                f"{country}: Parsed {stats['keyword_count']} keywords "
                f"(filtered {stats.get('keyword_filtered', 0)} of {stats.get('keyword_total_parsed', stats['keyword_count'])}), "
                f"{stats['rank_entries']} rank entries "
                f"({stats['date_count']} dates: {stats['date_range_start']} to {stats['date_range_end']})"
            )

            # --- Upsert keywords ---
            kw_count = upsert_keywords(keyword_records)
            logger.info(f"{country}: Upserted {kw_count} keywords")

            # Wait for DB sync before fetching IDs
            wait_for_db_sync(3)

            # --- Fetch keyword IDs ---
            keyword_id_map = fetch_keyword_ids(marketplace_id)
            logger.info(f"{country}: Fetched {len(keyword_id_map)} keyword IDs")

            # --- Build rank records with keyword_id ---
            rank_records = []
            skipped = 0

            for key, data in merged_ranks.items():
                asin_upper, keyword_lower, date_col = key
                keyword_id = keyword_id_map.get((asin_upper, keyword_lower))

                if not keyword_id:
                    skipped += 1
                    continue

                rank_records.append({
                    "keyword_id": keyword_id,
                    "rank_date": date_col,
                    "organic_rank": data["organic_rank"],
                    "organic_out_of_range": data["organic_out_of_range"],
                    "sponsored_rank": data["sponsored_rank"],
                    "sponsored_out_of_range": data["sponsored_out_of_range"],
                    "marketplace_id": marketplace_id,
                    "child_asin": data["asin"],
                    "keyword": data["keyword"],
                    "import_id": import_id,
                })

            if skipped > 0:
                logger.warning(f"{country}: Skipped {skipped} rank entries (missing keyword_id)")

            # --- Upsert ranks ---
            rank_count = upsert_ranks(rank_records)
            logger.info(f"{country}: Upserted {rank_count} rank records")

            # --- Update import record ---
            update_import_record(
                import_id,
                status="completed",
                row_count=rank_count,
                message=f"Keywords: {kw_count} kept ({stats.get('keyword_filtered', 0)} filtered), Ranks: {rank_count}, Dates: {stats['date_count']}",
            )

            result["status"] = "completed"
            result["keywords"] = kw_count
            result["keywords_filtered"] = stats.get('keyword_filtered', 0)
            result["ranks"] = rank_count

        except Exception as e:
            # Update import record as failed
            update_import_record(
                import_id,
                status="failed",
                error=str(e)[:500],
            )
            raise

    except Exception as e:
        result["error"] = str(e)[:200]
        logger.error(f"{country}: FAILED - {e}")

    return result


def main():
    parser = argparse.ArgumentParser(description="ScaleInsights Daily Rankings Pull")
    parser.add_argument(
        "--country",
        type=str,
        help="Single country code (US, CA, UK, DE, FR, AU). Default: all countries.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to download (default: 7)",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        help="Start date YYYY-MM-DD (overrides --days). For backfill.",
    )
    parser.add_argument(
        "--to-date",
        type=str,
        help="End date YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download only, skip DB insert",
    )
    args = parser.parse_args()

    # Validate country
    countries = ALL_COUNTRIES
    if args.country:
        country = args.country.upper()
        if country not in ALL_COUNTRIES:
            logger.error(f"Invalid country: {country}. Valid: {ALL_COUNTRIES}")
            sys.exit(1)
        countries = [country]

    # Calculate date range
    today = datetime.now()
    if args.from_date:
        from_date = args.from_date
    else:
        from_date = (today - timedelta(days=args.days)).strftime("%Y-%m-%d")
    to_date = args.to_date if args.to_date else today.strftime("%Y-%m-%d")

    # Get credentials
    email = os.environ.get("SI_EMAIL")
    password = os.environ.get("SI_PASSWORD")

    if not email or not password:
        logger.error("Missing SI_EMAIL or SI_PASSWORD environment variables")
        sys.exit(1)

    alert = get_alert_manager()
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("ScaleInsights Daily Rankings Pull")
    logger.info("=" * 60)
    logger.info(f"Date: {today.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Date range: {from_date} to {to_date} ({args.days} days)")
    logger.info(f"Countries: {countries}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("")

    # --- Login ---
    scraper = ScaleInsightsScraper(email, password)
    try:
        scraper.login()
    except Exception as e:
        logger.error(f"Login failed: {e}")
        alert.alert_login_failure(str(e))
        sys.exit(1)

    # --- Process countries ---
    results = []
    total_keywords = 0
    total_ranks = 0

    for i, country in enumerate(countries):
        # Delay between countries (not before first)
        if i > 0:
            logger.info(f"Waiting {INTER_COUNTRY_DELAY}s before next country...")
            time.sleep(INTER_COUNTRY_DELAY)

        result = process_country(
            scraper, country, from_date, to_date, dry_run=args.dry_run
        )
        results.append(result)

        if result["status"] == "completed":
            total_keywords += result.get("keywords", 0)
            total_ranks += result.get("ranks", 0)
        else:
            alert.alert_country_failure(country, result.get("error", "Unknown error"))

    # --- Summary ---
    duration = time.time() - start_time
    completed = [r for r in results if r["status"] == "completed"]
    failed = [r for r in results if r["status"] == "failed"]

    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Countries: {len(completed)}/{len(results)} success")
    logger.info(f"Keywords: {total_keywords:,}")
    logger.info(f"Ranks: {total_ranks:,}")
    logger.info(f"Duration: {duration:.1f}s")

    for r in results:
        status_icon = "OK" if r["status"] == "completed" else "FAIL"
        logger.info(
            f"  [{status_icon}] {r['country']}: "
            f"{r.get('keywords', 0):,} keywords, {r.get('ranks', 0):,} ranks"
            + (f" - {r['error']}" if r.get("error") else "")
        )

    # Send Slack summary
    alert.send_summary(
        results,
        total_keywords=total_keywords,
        total_ranks=total_ranks,
        duration_seconds=duration,
    )

    # Exit with error if any countries failed
    if failed:
        logger.error(f"{len(failed)} country(ies) failed")
        sys.exit(1)

    logger.info("All done!")


if __name__ == "__main__":
    main()
