# ScaleInsights Automation

Automated daily download and import of ScaleInsights keyword ranking data into Supabase.

**Repo:** https://github.com/anuj29111/scaleinsights-automation
**Supabase Project:** `yawaopfqkkvdqtsagmng` (same as Chalkola ONE, region: `ap-south-1`)

---

## Architecture
- **GitHub Actions** runs daily at 6 AM UTC (also supports manual trigger)
- Logs into ScaleInsights web portal via `requests.Session()` + CSRF token extraction
- Downloads keyword ranking Excel files for 6 countries
- Parses with `python-calamine` (10-50x faster than pandas)
- Upserts into `si_keywords` + `si_daily_ranks` tables in Supabase
- Sends Slack Block Kit summary to `#chalkola-hub-alerts`
- 7-day rolling window (overlapping data safe via UPSERT on unique constraints)

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

**Note:** AE (UAE) removed — not tracked in ScaleInsights.

---

## Batch Sizes & Timing
- Keywords upsert: **500/batch**
- Ranks upsert: **2000/batch**
- Keyword ID fetch: **10000/page** (paginated with `.range()` only — NOT `.limit()` + `.range()`)
- DB sync wait: **3 seconds** between keyword upsert and ID fetch (race condition prevention)
- Inter-country delay: **5 seconds** (avoid rate limiting)

---

## ScaleInsights Portal Details
- **Base URL:** `https://portal.scaleinsights.com`
- **Login URL:** `{BASE_URL}/Identity/Account/Login`
- **Download URL:** `{BASE_URL}/KeywordRanking?countrycode={code}&from={YYYY-MM-DD}&to={YYYY-MM-DD}&handler=Excel`
- **Auth:** Email + password (NOT SSO). CSRF token extracted from hidden form fields via BeautifulSoup.
- **Session:** `requests.Session()` maintains cookies. Auto re-login on session expiry (redirect to Login page detected).
- **No date limit** — can download any date range (e.g., Jan 2024 to today).

---

## Excel File Format
- **2 sheets:** Organic, Sponsored
- **17 fixed columns:** ASIN, SKU, Title, Keyword, Tracked, Sales, ACOS, Conversion, Spent, Orders, Units, Clicks, Query Volume, Conversion Delta, Market Conversion, Asin Conversion, Purchase Share
- **Dynamic date columns:** YYYY-MM-DD format (newest-first in file, sorted oldest-first in parser)
- **Rank values:** Integer (1-306), `97+`/`25+`/`N+` (out of range → `None, True`), `None` (no data)
- **PPC metrics are same in both sheets** — keywords imported from Organic (overwrites Sponsored). Only ranks differ between sheets.
- **Processing order:** Sponsored first → Organic overwrites (Organic is preferred source for keyword metrics)
- **Dedup key:** `(asin.upper(), keyword.lower())` for keywords, `(asin, keyword, date)` for ranks

---

## Parser Logic (ported from scaleinsight_routes.py)
1. **Read Excel** via Calamine (write bytes to temp file → `CalamineWorkbook.from_path()` → delete)
2. **Build keyword records** from both sheets (Sponsored first, Organic overwrites)
3. **Upsert keywords** into `si_keywords` (500/batch, on_conflict `marketplace_id,child_asin,keyword`)
4. **Wait 3s** for DB sync
5. **Fetch keyword IDs** (paginated 10000/page, builds `(asin_upper, keyword_lower) → UUID` map)
6. **Build merged rank records** from both sheets (organic + sponsored into single row per keyword+date)
7. **Upsert ranks** into `si_daily_ranks` (2000/batch, on_conflict `keyword_id,rank_date`)

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
- **Schedule:** `cron: '0 6 * * *'` (daily 6 AM UTC = 11:30 AM IST)
- **Default:** 7-day rolling window, all 6 countries
- **Manual trigger:** `workflow_dispatch` with inputs: country, days, from_date, to_date, dry_run
- **Secrets:** `SI_EMAIL`, `SI_PASSWORD`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SLACK_WEBHOOK_URL`
- **Python:** 3.11 with pip cache
- **Timeout:** 60 minutes

---

## Current Data State (as of Feb 10, 2026)
| Country | Earliest | Latest | Days | Rank Rows |
|---------|----------|--------|------|-----------|
| US | 2025-01-01 | 2026-02-09 | 403 | 1,872,847 |
| UK | 2025-01-01 | 2026-02-10 | 406 | 1,252,245 |
| CA | 2025-01-01 | 2026-02-09 | 405 | 549,426 |
| DE | 2025-01-01 | 2026-02-10 | 406 | 296,050 |
| FR | 2025-07-03 | 2026-02-10 | 223 | 23,026 |
| AU | 2025-06-03 | 2026-02-10 | 252 | 14,112 |

**Note:** FR and AU start dates reflect when ScaleInsights tracking began for those countries (no data exists before those dates in ScaleInsights).

---

## Future Enhancements
- Add retry logic at the country level (currently retries only at download level)
- Consider parallel country downloads (currently sequential with 5s delay)
- Calibrate minimum file sizes after more runs (current thresholds are estimates)

---

## Development History
| Session | Date | Summary |
|---------|------|---------|
| 53 | Feb 10, 2026 | Initial build. 12 files, 1,526 lines. Scraper (login + download), parser (ported from Chalkola ONE), db (Supabase ops), alerting (Slack), orchestrator, GitHub Actions workflow. Tested locally with 6 sample files. Pushed to GitHub. |
| 54 | Feb 10, 2026 | Live deployment. Added all 5 GitHub Secrets. First US test run succeeded (10,757 keywords, 39,152 ranks). Fixed `data_imports` column bug (`notes` → `metadata` jsonb). Changed UK code from `GB` → `UK`. Removed AE (not in ScaleInsights). Changed default from 30 → 7 days. Added `--from-date`/`--to-date` CLI args + workflow inputs for backfill. Backfilled all 6 countries to current. Daily 7-day rolling window now active. |

---

*Last Updated: February 10, 2026 (Session 54 — Fully deployed. All 6 countries backfilled. Daily 7-day cron active at 6 AM UTC.)*
