"""
ScaleInsights Excel Parser
Parses keyword ranking Excel files using python-calamine.

Ported from chalkola-one-system/routes/scaleinsight_routes.py
"""

import io
import re
import logging
import tempfile
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from python_calamine import CalamineWorkbook

logger = logging.getLogger(__name__)

# The 17 fixed columns in every ScaleInsights Excel file
FIXED_COLUMNS = [
    'ASIN', 'SKU', 'Title', 'Keyword', 'Tracked',
    'Sales', 'ACOS', 'Conversion', 'Spent', 'Orders',
    'Units', 'Clicks', 'Query Volume', 'Conversion Delta',
    'Market Conversion', 'Asin Conversion', 'Purchase Share'
]

DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')


# =============================================================================
# HELPERS (ported from scaleinsight_routes.py)
# =============================================================================

def _safe_str(val, default='') -> str:
    """Safely convert value to string, handling None and NaN."""
    if val is None:
        return default
    if isinstance(val, float) and (val != val):  # NaN check
        return default
    return str(val).strip()


def _safe_numeric(val, default=None):
    """Safely convert value to float."""
    if val is None:
        return default
    if isinstance(val, float) and (val != val):  # NaN check
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=None):
    """Safely convert value to int."""
    if val is None:
        return default
    if isinstance(val, float) and (val != val):  # NaN check
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def parse_rank_value(val) -> Tuple[Optional[int], bool]:
    """
    Parse a ScaleInsight rank cell value.

    Returns:
        (rank, is_out_of_range)
        - Integer rank: (rank, False)
        - "97+": (None, True)
        - None/empty: (None, False)
    """
    if val is None or val == '':
        return None, False
    if isinstance(val, float) and (val != val):  # NaN check
        return None, False

    val_str = str(val).strip()
    if not val_str:
        return None, False

    # Out of range patterns: "97+", "25+", any "N+"
    if val_str.endswith('+'):
        return None, True

    try:
        rank = int(float(val_str))
        if 1 <= rank <= 306:
            return rank, False
        else:
            return None, True  # Out of expected range
    except (ValueError, TypeError):
        return None, False


def detect_date_columns(columns: list) -> Tuple[List[str], Dict[str, Any]]:
    """
    Identify date columns from the Excel header row.

    Returns:
        (date_strings, col_map)
        - date_strings: List of normalized YYYY-MM-DD strings
        - col_map: Dict mapping normalized date string -> original column value
    """
    date_strs = []
    col_map = {}

    for col in columns:
        col_str = str(col).strip()
        normalized = None

        # Direct YYYY-MM-DD string match
        if DATE_PATTERN.match(col_str):
            normalized = col_str
        # Handle Timestamp/datetime objects
        elif hasattr(col, 'strftime'):
            try:
                normalized = col.strftime('%Y-%m-%d')
            except Exception:
                pass
        # Handle "2025-12-31 00:00:00" format
        elif len(col_str) >= 10 and DATE_PATTERN.match(col_str[:10]):
            normalized = col_str[:10]

        if normalized:
            date_strs.append(normalized)
            col_map[normalized] = col

    return date_strs, col_map


# =============================================================================
# MAIN PARSER
# =============================================================================

def parse_excel(
    file_bytes: bytes,
    marketplace_id: str,
    import_id: str,
) -> Tuple[List[Dict], Dict[Tuple, Dict], List[str], Dict[str, int]]:
    """
    Parse a ScaleInsights ranking Excel file.

    Args:
        file_bytes: Raw Excel file bytes
        marketplace_id: UUID of the marketplace
        import_id: UUID of the data_imports record

    Returns:
        (keyword_records, merged_ranks, sorted_dates, stats)
        - keyword_records: List of keyword dicts for si_keywords upsert
        - merged_ranks: Dict of (asin, keyword, date) -> rank data
        - sorted_dates: Sorted list of date strings found
        - stats: Dict with counts

    Raises:
        ValueError on invalid file structure
    """
    # Write bytes to temp file for Calamine
    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.xlsx')
    try:
        os.write(tmp_fd, file_bytes)
        os.close(tmp_fd)

        wb = CalamineWorkbook.from_path(tmp_path)
        sheet_names = wb.sheet_names
    finally:
        os.unlink(tmp_path)

    # Validate sheets
    has_organic = 'Organic' in sheet_names
    has_sponsored = 'Sponsored' in sheet_names

    if not has_organic and not has_sponsored:
        raise ValueError(
            f"Excel file has no 'Organic' or 'Sponsored' sheet. "
            f"Found: {sheet_names}"
        )

    # Read sheet data
    # Re-open because CalamineWorkbook may need fresh handle
    tmp_fd2, tmp_path2 = tempfile.mkstemp(suffix='.xlsx')
    try:
        os.write(tmp_fd2, file_bytes)
        os.close(tmp_fd2)

        wb = CalamineWorkbook.from_path(tmp_path2)

        organic_data = wb.get_sheet_by_name('Organic').to_python() if has_organic else None
        sponsored_data = wb.get_sheet_by_name('Sponsored').to_python() if has_sponsored else None
    finally:
        os.unlink(tmp_path2)

    # Use primary sheet (Organic preferred) for headers
    primary_data = organic_data if organic_data else sponsored_data
    if not primary_data or len(primary_data) < 2:
        raise ValueError("Primary sheet is empty or has no data rows")

    headers = [str(h).strip() if h is not None else '' for h in primary_data[0]]

    # Build column index map
    col_indices = {}
    for i, h in enumerate(headers):
        col_indices[h] = i

    # Validate fixed columns exist
    missing_cols = []
    for fc in FIXED_COLUMNS[:5]:  # At minimum need ASIN, SKU, Title, Keyword, Tracked
        if fc not in col_indices:
            missing_cols.append(fc)

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Detect date columns
    date_columns, date_col_map = detect_date_columns(headers)

    if not date_columns:
        raise ValueError("No date columns found in headers")

    # Also add date columns to col_indices (they may already be there as strings)
    for date_str, original_col in date_col_map.items():
        if date_str not in col_indices:
            original_str = str(original_col)
            if original_str in col_indices:
                col_indices[date_str] = col_indices[original_str]

    sorted_dates = sorted(date_columns)
    logger.info(
        f"Found {len(sorted_dates)} date columns: "
        f"{sorted_dates[0]} to {sorted_dates[-1]}"
    )

    # --- Step 1: Build keyword records from BOTH sheets ---
    keyword_map = {}  # (asin_upper, keyword_lower) -> kw_record

    # Process Sponsored FIRST so Organic overwrites (preferred source)
    for sheet_data, sheet_name in [
        (sponsored_data, "Sponsored"),
        (organic_data, "Organic"),
    ]:
        if not sheet_data:
            continue

        rows = sheet_data[1:]  # Skip header
        is_organic = sheet_name == "Organic"

        for row in rows:
            asin = _safe_str(row[col_indices.get('ASIN', 0)])
            keyword_text = _safe_str(row[col_indices.get('Keyword', 3)])

            if not asin or not keyword_text:
                continue

            key = (asin.upper(), keyword_text.lower())

            # For Sponsored: only set if not already present
            # For Organic: always overwrite (preferred source)
            if not is_organic and key in keyword_map:
                continue

            tracked_val = _safe_str(row[col_indices.get('Tracked', 4)]).lower()
            keyword_map[key] = {
                'marketplace_id': marketplace_id,
                'child_asin': asin,
                'keyword': keyword_text,
                'sku': _safe_str(row[col_indices.get('SKU', 1)]) or None,
                'title': (_safe_str(row[col_indices.get('Title', 2)])[:500]) or None,
                'tracked': tracked_val == 'yes',
                'sales': _safe_numeric(row[col_indices.get('Sales', 5)]),
                'acos': _safe_numeric(row[col_indices.get('ACOS', 6)]),
                'conversion': _safe_numeric(row[col_indices.get('Conversion', 7)]),
                'spent': _safe_numeric(row[col_indices.get('Spent', 8)]),
                'orders': _safe_int(row[col_indices.get('Orders', 9)]),
                'units': _safe_int(row[col_indices.get('Units', 10)]),
                'clicks': _safe_int(row[col_indices.get('Clicks', 11)]),
                'query_volume': _safe_int(row[col_indices.get('Query Volume', 12)]),
                'conversion_delta': _safe_numeric(row[col_indices.get('Conversion Delta', 13)]),
                'market_conversion': _safe_numeric(row[col_indices.get('Market Conversion', 14)]),
                'asin_conversion': _safe_numeric(row[col_indices.get('Asin Conversion', 15)]),
                'purchase_share': _safe_numeric(row[col_indices.get('Purchase Share', 16)]),
                'metrics_period_start': sorted_dates[0] if sorted_dates else None,
                'metrics_period_end': sorted_dates[-1] if sorted_dates else None,
                'import_id': import_id,
                'updated_at': datetime.utcnow().isoformat(),
            }

    # --- Filter: keep only tracked=YES or spent>0 ---
    all_keywords = list(keyword_map.values())
    keyword_records = [
        kw for kw in all_keywords
        if kw.get('tracked', False) or (kw.get('spent') is not None and kw['spent'] > 0)
    ]
    filtered_count = len(all_keywords) - len(keyword_records)
    logger.info(
        f"Extracted {len(all_keywords)} unique keywords, "
        f"kept {len(keyword_records)} after filtering "
        f"(removed {filtered_count} with no spend and not tracked)"
    )

    # Set of kept keyword keys for rank filtering
    kept_keyword_keys = {(kw['child_asin'].upper(), kw['keyword'].lower()) for kw in keyword_records}

    # --- Step 2: Build MERGED rank data from both sheets ---
    merged_ranks = {}  # (asin_upper, keyword_lower, date) -> rank dict
    organic_count = 0
    sponsored_count = 0

    for sheet_data, sheet_name in [
        (organic_data, "Organic"),
        (sponsored_data, "Sponsored"),
    ]:
        if not sheet_data:
            continue

        rows = sheet_data[1:]  # Skip header
        is_organic = sheet_name == "Organic"

        for row in rows:
            asin = _safe_str(row[col_indices.get('ASIN', 0)])
            keyword_text = _safe_str(row[col_indices.get('Keyword', 3)])

            if not asin or not keyword_text:
                continue

            for date_col in sorted_dates:
                # Get date column index
                date_idx = col_indices.get(date_col)
                if date_idx is None:
                    original_col = date_col_map.get(date_col, date_col)
                    date_idx = col_indices.get(str(original_col))

                if date_idx is None or date_idx >= len(row):
                    continue

                cell_val = row[date_idx]
                rank, is_oor = parse_rank_value(cell_val)

                if rank is None and not is_oor:
                    continue  # No data for this cell

                key = (asin.upper(), keyword_text.lower(), date_col)
                if key not in merged_ranks:
                    merged_ranks[key] = {
                        'asin': asin,
                        'keyword': keyword_text,
                        'date': date_col,
                        'organic_rank': None,
                        'organic_out_of_range': False,
                        'sponsored_rank': None,
                        'sponsored_out_of_range': False,
                    }

                if is_organic:
                    merged_ranks[key]['organic_rank'] = rank
                    merged_ranks[key]['organic_out_of_range'] = is_oor
                    organic_count += 1
                else:
                    merged_ranks[key]['sponsored_rank'] = rank
                    merged_ranks[key]['sponsored_out_of_range'] = is_oor
                    sponsored_count += 1

    logger.info(
        f"Built {len(merged_ranks)} merged rank entries "
        f"(organic: {organic_count}, sponsored: {sponsored_count})"
    )

    # Filter rank entries to only kept keywords
    pre_filter = len(merged_ranks)
    merged_ranks = {k: v for k, v in merged_ranks.items() if (k[0], k[1]) in kept_keyword_keys}
    rank_filtered = pre_filter - len(merged_ranks)
    if rank_filtered > 0:
        logger.info(f"Filtered {rank_filtered} rank entries for skipped keywords")

    stats = {
        'keyword_count': len(keyword_records),
        'keyword_total_parsed': len(all_keywords),
        'keyword_filtered': filtered_count,
        'rank_entries': len(merged_ranks),
        'organic_ranks': organic_count,
        'sponsored_ranks': sponsored_count,
        'date_range_start': sorted_dates[0] if sorted_dates else None,
        'date_range_end': sorted_dates[-1] if sorted_dates else None,
        'date_count': len(sorted_dates),
    }

    return keyword_records, merged_ranks, sorted_dates, stats
