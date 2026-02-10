# ScaleInsights Automation

Automated daily download and import of ScaleInsights keyword ranking data into Supabase.

**Repo:** https://github.com/anuj29111/scaleinsights-automation
**Supabase Project:** `yawaopfqkkvdqtsagmng` (same as Chalkola ONE, region: `ap-south-1`)

---

## Architecture
- **GitHub Actions** runs daily at 6 AM UTC (also supports manual trigger)
- Logs into ScaleInsights web portal via `requests.Session()` + CSRF token extraction
- Downloads keyword ranking Excel files for 7 countries
- Parses with `python-calamine` (10-50x faster than pandas)
- Upserts into `si_keywords` + `si_daily_ranks` tables in Supabase
- Sends Slack Block Kit summary to `#chalkola-hub-alerts`
- 30-day rolling window (overlapping data safe via UPSERT on unique constraints)

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
├── Sample/                     # Sample Excel files for testing (6 countries)
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
| UK | `GB` ⚠️ | `b2c3d4e5-58cc-4372-a567-0e02b2c3d481` | 100 KB |
| DE | `DE` | `c3d4e5f6-58cc-4372-a567-0e02b2c3d482` | 50 KB |
| FR | `FR` | `d4e5f6a7-58cc-4372-a567-0e02b2c3d483` | 20 KB |
| AE | `AE` | `e5f6a7b8-58cc-4372-a567-0e02b2c3d484` | 20 KB |
| AU | `AU` | `f6a7b8c9-58cc-4372-a567-0e02b2c3d485` | 20 KB |

⚠️ **UK uses `GB` in download URL** — set based on Manus AI reference script. Needs validation on first real run. If download fails, try changing to `UK` in `SI_COUNTRY_CODES` dict in `db.py`.

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

# Single country, 7 days
python scripts/pull_rankings.py --country US --days 7

# All countries, 30 days (default)
python scripts/pull_rankings.py

# Dry run (download only, no DB insert)
python scripts/pull_rankings.py --country US --days 7 --dry-run
```

---

## GitHub Actions Workflow
- **Schedule:** `cron: '0 6 * * *'` (daily 6 AM UTC)
- **Manual trigger:** `workflow_dispatch` with inputs: country, days, dry_run
- **Secrets needed:** `SI_EMAIL`, `SI_PASSWORD`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SLACK_WEBHOOK_URL`
- **Python:** 3.11 with pip cache
- **Timeout:** 60 minutes

---

## Pending Tasks

### 1. Add GitHub Secrets (IMMEDIATE — user must do)
Go to https://github.com/anuj29111/scaleinsights-automation/settings/secrets/actions and add:
- `SI_EMAIL` — `info@chalkola.com`
- `SI_PASSWORD` — ScaleInsights login password
- `SUPABASE_URL` — `https://yawaopfqkkvdqtsagmng.supabase.co`
- `SUPABASE_SERVICE_KEY` — service role key (from Railway env vars or Supabase dashboard)
- `SLACK_WEBHOOK_URL` — the `#chalkola-hub-alerts` webhook URL

### 2. Test with Manual Trigger (NEXT)
- Actions tab → "ScaleInsights Daily Rankings Pull" → "Run workflow"
- Start with: country=`US`, days=`7`
- Verify: download succeeds, parse works, data appears in `si_keywords` + `si_daily_ranks`
- Check `data_imports` table for new `SCALEINSIGHT` type record

### 3. Validate UK Country Code
- SI may use `GB` or `UK` in download URL
- If UK fails, update `SI_COUNTRY_CODES["UK"]` in `scripts/utils/db.py` from `"GB"` to `"UK"`

### 4. Calibrate Minimum File Sizes
- `COUNTRY_MIN_SIZE` in `db.py` are estimates based on sample files
- After first real run with full data (not sample), adjust thresholds if valid files are being rejected

### 5. Full Run All 7 Countries
- After single-country test passes, trigger with no country filter (processes all 7)
- Monitor Slack for summary notification

### 6. Future Enhancements
- Add UAE sample file (currently missing from Sample/ directory)
- Consider adding `--from-date` / `--to-date` CLI args for custom date ranges
- Add retry logic at the country level (currently retries only at download level)
- Consider parallel country downloads (currently sequential with 5s delay)

---

## Development History
| Session | Date | Summary |
|---------|------|---------|
| 53 | Feb 10, 2026 | Initial build. 12 files, 1,526 lines. Scraper (login + download), parser (ported from Chalkola ONE), db (Supabase ops), alerting (Slack), orchestrator, GitHub Actions workflow. Tested locally with 6 sample files. Pushed to GitHub. |

---

*Last Updated: February 10, 2026 (Session 53 — Initial build complete. Pending: GitHub Secrets + first live test run)*
