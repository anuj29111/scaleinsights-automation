# ScaleInsights Automation

Automated daily download and import of ScaleInsights keyword ranking data into Supabase.

**Repo:** https://github.com/anuj29111/scaleinsights-automation
**Supabase Project:** `yawaopfqkkvdqtsagmng` (same as Chalkola ONE, region: `ap-south-1`)
**Status:** Fully deployed. Daily cron active. All 6 countries backfilled from Jan 2025.

---

## Architecture
- **GitHub Actions** runs daily on split schedule: EU/AU at 6 AM UTC, NA at 10 AM UTC (also supports manual trigger)
- Logs into ScaleInsights web portal via `requests.Session()` + CSRF token extraction
- Downloads keyword ranking Excel files for 6 countries
- Parses with `python-calamine` (10-50x faster than pandas)
- Upserts into `si_keywords` + `si_daily_ranks` tables in Supabase
- Sends Slack Block Kit summary to `#chalkola-hub-alerts`
- 7-day rolling window (overlapping data safe via UPSERT on unique constraints)

---

## Critical Rules (Bug Prevention)
- **UK country code is `UK`** not `GB` — ScaleInsights uses `UK` in download URL
- **AE (UAE) is NOT in ScaleInsights** — do not add it back
- **`data_imports` table has `metadata` (jsonb) column** — NOT `notes`. Use `{"notes": message}` pattern.
- **Keyword ID fetch uses `.range()` only** — do NOT combine `.limit()` + `.range()` (Supabase ignores limit with range)
- **3-second wait required** between keyword upsert and ID fetch (race condition — IDs not immediately queryable)
- **ScaleInsights has NO date limit** — any date range works (tested Jan 2024 to today, 29MB file)
- **FR and AU tracking started mid-2025** — no data exists before ~Jul 2025 (FR) / ~Jun 2025 (AU) in ScaleInsights
- **Processing order: Sponsored first → Organic overwrites** — Organic is preferred source for keyword metrics

---

## File Structure
```
Scale Insights/
├── .github/workflows/
│   └── daily-pull.yml          # GitHub Actions: daily 6 AM UTC + manual trigger
├── scripts/
│   ├── pull_rankings.py        # Main orchestrator (login → download → parse → upsert)
│   └── utils/
│       ├── scraper.py          # ScaleInsights login + Excel download (retry, re-login)
│       ├── parser.py           # Excel parsing (ported from Chalkola ONE scaleinsight_routes.py)
│       ├── db.py               # Supabase operations (upserts, import tracking, marketplace UUIDs)
│       └── alerting.py         # Slack webhook notifications (Block Kit)
├── Sample/                     # Sample Excel files for testing
├── .env.example
├── .gitignore
├── requirements.txt
└── CLAUDE.md
```

---

## Key Tables (in Supabase)
| Table | Purpose | Unique Constraint |
|-------|---------|-------------------|
| `si_keywords` | Keyword master (ASIN + keyword + PPC metrics) | `marketplace_id, child_asin, keyword` |
| `si_daily_ranks` | Daily organic + sponsored rank values | `keyword_id, rank_date` |
| `data_imports` | Import tracking (status, row count, dates) | — |

---

## Country Configuration
| Country | SI Download Code | Marketplace UUID | Min File Size |
|---------|-----------------|-----------------|---------------|
| US | `US` | `f47ac10b-58cc-4372-a567-0e02b2c3d479` | 500 KB |
| CA | `CA` | `a1b2c3d4-58cc-4372-a567-0e02b2c3d480` | 100 KB |
| UK | `UK` | `b2c3d4e5-58cc-4372-a567-0e02b2c3d481` | 100 KB |
| DE | `DE` | `c3d4e5f6-58cc-4372-a567-0e02b2c3d482` | 50 KB |
| FR | `FR` | `d4e5f6a7-58cc-4372-a567-0e02b2c3d483` | 20 KB |
| AU | `AU` | `f6a7b8c9-58cc-4372-a567-0e02b2c3d485` | 20 KB |

---

## Batch Sizes & Timing
- Keywords upsert: **500/batch**
- Ranks upsert: **2000/batch**
- Keyword ID fetch: **10000/page**
- DB sync wait: **3 seconds** between keyword upsert and ID fetch
- Inter-country delay: **5 seconds**

---

## ScaleInsights Portal Details
- **Base URL:** `https://portal.scaleinsights.com`
- **Login URL:** `{BASE_URL}/Identity/Account/Login`
- **Download URL:** `{BASE_URL}/KeywordRanking?countrycode={code}&from={YYYY-MM-DD}&to={YYYY-MM-DD}&handler=Excel`
- **Auth:** Email + password (NOT SSO). CSRF token extracted from hidden form fields via BeautifulSoup.
- **Session:** `requests.Session()` maintains cookies. Auto re-login on session expiry (redirect to Login page detected).
- **No date limit** — can download any date range.

---

## Excel File Format
- **2 sheets:** Organic, Sponsored
- **17 fixed columns:** ASIN, SKU, Title, Keyword, Tracked, Sales, ACOS, Conversion, Spent, Orders, Units, Clicks, Query Volume, Conversion Delta, Market Conversion, Asin Conversion, Purchase Share
- **Dynamic date columns:** YYYY-MM-DD format (newest-first in file, sorted oldest-first in parser)
- **Rank values:** Integer (1-306), `97+`/`25+`/`N+` (out of range → `None, True`), `None` (no data)
- **Dedup key:** `(asin.upper(), keyword.lower())` for keywords, `(asin, keyword, date)` for ranks

---

## Running Locally
```bash
pip install -r requirements.txt
export SI_EMAIL=info@chalkola.com
export SI_PASSWORD=your_password
export SUPABASE_URL=https://yawaopfqkkvdqtsagmng.supabase.co
export SUPABASE_SERVICE_KEY=your_service_key

# All countries, 7-day rolling window (default)
python scripts/pull_rankings.py

# Single country, 7 days
python scripts/pull_rankings.py --country US --days 7

# Custom date range (for backfill)
python scripts/pull_rankings.py --country FR --from-date 2025-01-01 --to-date 2026-02-10

# Dry run (download only, no DB insert)
python scripts/pull_rankings.py --country US --days 7 --dry-run
```

---

## GitHub Actions Workflow
- **Schedule:** Split by region to ensure complete day data:
  - `0 6 * * *` (6 AM UTC = 11:30 AM IST) → **EU + AU** (UK, DE, FR, AU)
  - `0 10 * * *` (10 AM UTC = 3:30 PM IST) → **NA** (US, CA)
- **Default:** 7-day rolling window
- **Manual trigger:** `workflow_dispatch` with inputs: country, days, from_date, to_date, dry_run
- **Secrets:** `SI_EMAIL`, `SI_PASSWORD`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SLACK_WEBHOOK_URL`
- **Python:** 3.11 with pip cache
- **Timeout:** 60 minutes

---

## Downstream Consumers

### Chalkola ONE (`/Users/anuj/Desktop/Github/Chalkola ONE/`)

Reads `si_keywords` and `si_daily_ranks` for Analysis Tab (keyword intelligence) and Products page.

**Critical join rules Chalkola ONE depends on:**
- `si_daily_ranks.child_asin` column exists but is NOT populated — must join through `si_keywords` via `keyword_id`
- `si_daily_ranks` has 2.3M+ rows — ALWAYS filter by marketplace + date range
- Join columns: `si_daily_ranks.rank_date` (NOT `date`), `si_keywords.child_asin` (NOT `asin`)
- `si_keywords` columns: `query_volume` (NOT `search_volume`), `keyword` (NOT `keyword_text`)
- MUST join `products` table with `marketplace_id` filter to avoid cross-marketplace duplication
- Chalkola ONE uses `execute_readonly_query` RPC (returns jsonb) for SELECT queries. Parameter: `query_text`

**If you change table schemas or unique constraints**, check Chalkola ONE CLAUDE.md for downstream impact.

### SP-API (`/Users/anuj/Desktop/Github/SP-API/`)

No direct dependency — SP-API and ScaleInsights write to the same Supabase project independently. SP-API pulls sales/traffic/inventory/financial data; ScaleInsights pulls keyword ranking data.

---

## Pending Tasks
None — fully deployed and automated.

---

## Future Enhancements
- Add retry logic at the country level (currently retries only at download level)
- Consider parallel country downloads (currently sequential with 5s delay)
- Calibrate minimum file sizes after more runs (current thresholds are estimates)

---

*Last Updated: February 10, 2026 (Session 55)*
