#!/usr/bin/env python3
"""
ScaleInsights Health Check

Validates daily keyword ranking imports and auto-remedies missing/anomalous data.
Compares today's import counts against 7-day median benchmarks per country.

Usage:
    # Check EU+AU batch
    python scripts/health_check.py --batch eu_au

    # Check all countries, no auto-fix
    python scripts/health_check.py --batch all --no-fix

    # Custom deviation threshold (default: 5%)
    python scripts/health_check.py --threshold 0.10
"""

import os
import sys
import logging
import argparse
import subprocess
import time
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Dict, List, Optional

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils.db import MARKETPLACE_UUIDS, get_supabase_client
from scripts.utils.alerting import get_alert_manager

logger = logging.getLogger(__name__)

BATCHES = {
    "eu_au": ["UK", "DE", "FR", "AU"],
    "na": ["US", "CA"],
    "all": ["US", "CA", "UK", "DE", "FR", "AU"],
}

# Reverse lookup: marketplace_id -> country code
MARKETPLACE_TO_COUNTRY = {v: k for k, v in MARKETPLACE_UUIDS.items()}


def fetch_todays_imports(client, marketplace_ids: List[str]) -> Dict[str, dict]:
    """Get latest completed import per marketplace for today (UTC)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    result = (
        client.table("data_imports")
        .select("marketplace_id, row_count, created_at, metadata")
        .eq("import_type", "SCALEINSIGHT")
        .eq("status", "completed")
        .gte("created_at", f"{today}T00:00:00+00:00")
        .in_("marketplace_id", marketplace_ids)
        .order("created_at", desc=True)
        .execute()
    )

    # Keep latest per marketplace
    latest = {}
    for row in result.data:
        mid = row["marketplace_id"]
        if mid not in latest:
            latest[mid] = row
    return latest


def fetch_todays_rank_counts(client, marketplace_ids: List[str]) -> Dict[str, int]:
    """Count si_daily_ranks rows for today per marketplace (secondary validation)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Query rank counts grouped by marketplace
    counts = {}
    for mid in marketplace_ids:
        result = (
            client.table("si_daily_ranks")
            .select("id", count="exact")
            .eq("marketplace_id", mid)
            .eq("rank_date", today)
            .execute()
        )
        counts[mid] = result.count or 0
    return counts


def fetch_benchmarks(
    client, marketplace_ids: List[str], lookback_days: int = 7
) -> Dict[str, float]:
    """Get median row_count per marketplace over last N days (excluding today)."""
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=lookback_days)).isoformat()
    end = today.isoformat()

    result = (
        client.table("data_imports")
        .select("marketplace_id, row_count")
        .eq("import_type", "SCALEINSIGHT")
        .eq("status", "completed")
        .gte("created_at", f"{start}T00:00:00+00:00")
        .lt("created_at", f"{end}T00:00:00+00:00")
        .in_("marketplace_id", marketplace_ids)
        .execute()
    )

    grouped: Dict[str, List[int]] = {}
    for row in result.data:
        mid = row["marketplace_id"]
        if row["row_count"] is not None and row["row_count"] > 0:
            grouped.setdefault(mid, []).append(row["row_count"])

    return {mid: median(counts) for mid, counts in grouped.items() if counts}


def classify_countries(
    countries: List[str],
    todays_imports: Dict[str, dict],
    todays_ranks: Dict[str, int],
    benchmarks: Dict[str, float],
    threshold: float,
) -> List[dict]:
    """Classify each country as OK, MISSING, or ANOMALOUS."""
    results = []
    for country in countries:
        mid = MARKETPLACE_UUIDS[country]
        entry = {"country": country, "marketplace_id": mid}

        has_import = mid in todays_imports
        today_ranks = todays_ranks.get(mid, 0)
        bench = benchmarks.get(mid)

        if not has_import and today_ranks == 0:
            entry["status"] = "MISSING"
            entry["today_count"] = 0
            entry["today_ranks"] = 0
            entry["benchmark"] = bench or 0
            entry["deviation"] = None
        else:
            today_count = (todays_imports.get(mid, {}).get("row_count") or 0)
            # Use the higher of import row_count and actual rank count
            effective_count = max(today_count, today_ranks)
            entry["today_count"] = today_count
            entry["today_ranks"] = today_ranks

            if not bench or bench == 0:
                entry["status"] = "OK"  # No benchmark available
                entry["benchmark"] = 0
                entry["deviation"] = None
            else:
                deviation = (bench - effective_count) / bench if bench > 0 else 0
                entry["deviation"] = deviation
                entry["benchmark"] = bench
                # Only flag if count is significantly LOWER than benchmark
                entry["status"] = "ANOMALOUS" if deviation > threshold else "OK"

        results.append(entry)
    return results


def auto_fix(countries_to_fix: List[str]) -> Dict[str, bool]:
    """Re-run pull_rankings.py for each problem country."""
    fix_results = {}
    script = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "pull_rankings.py"
    )

    for country in countries_to_fix:
        logger.info(f"Auto-fix: re-pulling {country}...")
        try:
            result = subprocess.run(
                [sys.executable, script, "--country", country, "--days", "7"],
                timeout=1800,  # 30 min per country max
                capture_output=True,
                text=True,
            )
            success = result.returncode == 0
            fix_results[country] = success
            if success:
                logger.info(f"Auto-fix {country}: SUCCESS")
            else:
                logger.error(
                    f"Auto-fix {country}: FAILED (exit {result.returncode})"
                )
                # Log last 500 chars of stderr
                if result.stderr:
                    logger.error(f"  stderr: {result.stderr[-500:]}")
        except subprocess.TimeoutExpired:
            logger.error(f"Auto-fix {country}: TIMED OUT (30 min)")
            fix_results[country] = False
        except Exception as e:
            logger.error(f"Auto-fix {country}: ERROR - {e}")
            fix_results[country] = False

        # Brief delay between re-pulls
        if len(countries_to_fix) > 1:
            time.sleep(5)

    return fix_results


def main():
    parser = argparse.ArgumentParser(description="ScaleInsights Health Check")
    parser.add_argument(
        "--batch",
        choices=["eu_au", "na", "all"],
        default="all",
        help="Which country batch to check (default: all)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.05,
        help="Deviation threshold for anomaly detection (default: 0.05 = 5%%)",
    )
    parser.add_argument(
        "--no-fix",
        action="store_true",
        help="Check only, skip auto-remediation",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Lookback days for benchmark (default: 7)",
    )
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    countries = BATCHES[args.batch]
    marketplace_ids = [MARKETPLACE_UUIDS[c] for c in countries]
    alert = get_alert_manager()

    logger.info(f"Health check: batch={args.batch}, countries={countries}, "
                f"threshold={args.threshold:.0%}, auto-fix={not args.no_fix}")

    # 1. Connect to Supabase
    try:
        client = get_supabase_client()
    except Exception as e:
        logger.error(f"Failed to connect to Supabase: {e}")
        sys.exit(1)

    # 2. Fetch today's data and benchmarks
    logger.info("Fetching today's imports and rank counts...")
    todays_imports = fetch_todays_imports(client, marketplace_ids)
    todays_ranks = fetch_todays_rank_counts(client, marketplace_ids)
    benchmarks = fetch_benchmarks(client, marketplace_ids, args.days)

    logger.info(f"Today's imports: {len(todays_imports)} marketplaces")
    logger.info(f"Benchmarks (7-day median): {len(benchmarks)} marketplaces")

    # 3. Classify
    results = classify_countries(
        countries, todays_imports, todays_ranks, benchmarks, args.threshold
    )

    ok = [r for r in results if r["status"] == "OK"]
    missing = [r for r in results if r["status"] == "MISSING"]
    anomalous = [r for r in results if r["status"] == "ANOMALOUS"]

    for r in results:
        dev_str = f"{r['deviation']:.1%}" if r["deviation"] is not None else "N/A"
        logger.info(
            f"  {r['country']}: {r['status']} — "
            f"today={r['today_count']:,} ranks_actual={r.get('today_ranks', 0):,} "
            f"benchmark={r['benchmark']:,.0f} deviation={dev_str}"
        )

    # 4. Auto-fix if needed
    fixed_countries = {}
    if (missing or anomalous) and not args.no_fix:
        countries_to_fix = [r["country"] for r in missing + anomalous]
        logger.info(f"Auto-fixing {len(countries_to_fix)} countries: {countries_to_fix}")
        fixed_countries = auto_fix(countries_to_fix)

        # Re-validate after fix
        logger.info("Re-validating after auto-fix...")
        time.sleep(5)  # Wait for DB sync
        todays_imports = fetch_todays_imports(client, marketplace_ids)
        todays_ranks = fetch_todays_rank_counts(client, marketplace_ids)
        results = classify_countries(
            countries, todays_imports, todays_ranks, benchmarks, args.threshold
        )

        ok = [r for r in results if r["status"] == "OK"]
        missing = [r for r in results if r["status"] == "MISSING"]
        anomalous = [r for r in results if r["status"] == "ANOMALOUS"]

        for r in results:
            dev_str = f"{r['deviation']:.1%}" if r["deviation"] is not None else "N/A"
            logger.info(
                f"  {r['country']}: {r['status']} — "
                f"today={r['today_count']:,} benchmark={r['benchmark']:,.0f} "
                f"deviation={dev_str}"
            )

    # 5. Send Slack alert
    still_broken = [r["country"] for r in missing + anomalous]
    alert.alert_health_check(results, fixed_countries, still_broken)

    # 6. Exit code
    if still_broken:
        logger.error(f"Health check FAILED: {still_broken} still have issues")
        sys.exit(1)
    else:
        logger.info("Health check PASSED: all countries OK")
        sys.exit(0)


if __name__ == "__main__":
    main()
