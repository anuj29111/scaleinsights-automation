"""
Supabase Database Module
Handles all database operations for ScaleInsights ranking data.

Pattern follows SP-API/scripts/utils/db.py
"""

import os
import json
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from supabase import create_client, Client

logger = logging.getLogger(__name__)

# Supabase client singleton
_supabase_client: Optional[Client] = None

# Marketplace UUID mapping (from Supabase marketplaces table)
MARKETPLACE_UUIDS = {
    "US": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "CA": "a1b2c3d4-58cc-4372-a567-0e02b2c3d480",
    "UK": "b2c3d4e5-58cc-4372-a567-0e02b2c3d481",
    "DE": "c3d4e5f6-58cc-4372-a567-0e02b2c3d482",
    "FR": "d4e5f6a7-58cc-4372-a567-0e02b2c3d483",
    "AU": "f6a7b8c9-58cc-4372-a567-0e02b2c3d485",
}

# ScaleInsights country code mapping (what SI uses in download URLs)
SI_COUNTRY_CODES = {
    "US": "US",
    "CA": "CA",
    "UK": "UK",
    "DE": "DE",
    "FR": "FR",
    "AU": "AU",
}

# Minimum expected file size per country (bytes) — smaller means likely error page
COUNTRY_MIN_SIZE = {
    "US": 500 * 1024,    # 500KB
    "CA": 100 * 1024,    # 100KB
    "UK": 100 * 1024,    # 100KB
    "DE": 50 * 1024,     # 50KB
    "FR": 20 * 1024,     # 20KB
    "AU": 20 * 1024,     # 20KB
}


def get_supabase_client() -> Client:
    """Get or create Supabase client singleton."""
    global _supabase_client

    if _supabase_client is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")

        if not url or not key:
            raise ValueError(
                "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables"
            )

        _supabase_client = create_client(url, key)

    return _supabase_client


# =============================================================================
# IMPORT TRACKING
# =============================================================================

def create_import_record(
    marketplace_id: str,
    date_min: str,
    date_max: str,
    filename: str,
) -> str:
    """
    Create a data_imports record for tracking this import.

    Returns:
        Import ID (UUID string)
    """
    client = get_supabase_client()

    result = client.table("data_imports").insert({
        "marketplace_id": marketplace_id,
        "import_type": "SCALEINSIGHT",
        "period_start_date": date_min,
        "period_end_date": date_max,
        "period_type": "daily",
        "status": "processing",
        "file_name": filename,
    }).execute()

    import_id = result.data[0]["id"]
    logger.info(f"Created import record: {import_id}")
    return import_id


def update_import_record(
    import_id: str,
    status: str,
    row_count: Optional[int] = None,
    message: Optional[str] = None,
    error: Optional[str] = None,
):
    """Update a data_imports record with status/counts."""
    client = get_supabase_client()

    update_data = {"status": status}
    if row_count is not None:
        update_data["row_count"] = row_count
    if message:
        update_data["metadata"] = {"notes": message}
    if error:
        update_data["error_message"] = error
    if status in ("completed", "failed"):
        update_data["completed_at"] = datetime.utcnow().isoformat()

    client.table("data_imports").update(update_data).eq("id", import_id).execute()
    logger.info(f"Updated import {import_id}: status={status}")


# =============================================================================
# KEYWORD OPERATIONS
# =============================================================================

def upsert_keywords(keyword_records: List[Dict], batch_size: int = 500) -> int:
    """
    Upsert keyword records into si_keywords.

    Args:
        keyword_records: List of keyword dicts
        batch_size: Records per batch (default 500)

    Returns:
        Number of records upserted
    """
    if not keyword_records:
        return 0

    client = get_supabase_client()
    upserted = 0

    for i in range(0, len(keyword_records), batch_size):
        batch = keyword_records[i:i + batch_size]
        # Serialize to handle any non-JSON-safe types
        batch_json = json.loads(json.dumps(batch, default=str))

        try:
            client.table("si_keywords").upsert(
                batch_json,
                on_conflict="marketplace_id,child_asin,keyword"
            ).execute()
            upserted += len(batch)
            logger.info(
                f"Upserted keyword batch {i // batch_size + 1}: "
                f"{len(batch)} rows (total: {upserted})"
            )
        except Exception as e:
            logger.error(f"Keyword upsert failed at batch {i // batch_size + 1}: {e}")
            raise

    return upserted


def fetch_keyword_ids(marketplace_id: str) -> Dict[Tuple[str, str], str]:
    """
    Fetch all keyword IDs for a marketplace.

    Returns:
        Dict mapping (asin_upper, keyword_lower) -> UUID

    Uses paginated queries with .range() only (not .limit() + .range()).
    """
    client = get_supabase_client()
    keyword_id_map = {}

    offset = 0
    page_size = 10000

    while True:
        result = client.table("si_keywords").select(
            "id, child_asin, keyword"
        ).eq(
            "marketplace_id", marketplace_id
        ).range(offset, offset + page_size - 1).execute()

        rows = result.data or []
        for kw in rows:
            key = (kw["child_asin"].upper(), kw["keyword"].lower())
            keyword_id_map[key] = kw["id"]

        logger.info(
            f"Fetched {len(rows)} keyword IDs "
            f"(offset {offset}, total: {len(keyword_id_map)})"
        )

        if len(rows) < page_size:
            break
        offset += page_size

    return keyword_id_map


# =============================================================================
# RANK OPERATIONS
# =============================================================================

def upsert_ranks(rank_records: List[Dict], batch_size: int = 2000) -> int:
    """
    Upsert rank records into si_daily_ranks.

    Args:
        rank_records: List of rank dicts with keyword_id, rank_date, etc.
        batch_size: Records per batch (default 2000)

    Returns:
        Number of records upserted
    """
    if not rank_records:
        return 0

    client = get_supabase_client()
    upserted = 0

    for i in range(0, len(rank_records), batch_size):
        batch = rank_records[i:i + batch_size]
        batch_json = json.loads(json.dumps(batch, default=str))

        try:
            client.table("si_daily_ranks").upsert(
                batch_json,
                on_conflict="keyword_id,rank_date"
            ).execute()
            upserted += len(batch)
            logger.info(
                f"Upserted rank batch {i // batch_size + 1}: "
                f"{len(batch)} rows (total: {upserted})"
            )
        except Exception as e:
            logger.error(f"Rank upsert failed at batch {i // batch_size + 1}: {e}")
            raise

    return upserted


def wait_for_db_sync(seconds: int = 3):
    """
    Wait for DB to sync after keyword upserts.

    Prevents race condition where keyword ID fetch starts
    before all keyword batches are committed.
    """
    logger.info(f"Waiting {seconds}s for DB sync...")
    time.sleep(seconds)
