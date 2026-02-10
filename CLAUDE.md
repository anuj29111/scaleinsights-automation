# ScaleInsights Automation

Automated daily download and import of ScaleInsights keyword ranking data into Supabase.

## Architecture
- GitHub Actions runs daily at 6 AM UTC
- Logs into ScaleInsights web portal via requests session
- Downloads Excel files for 7 countries (US, CA, UK, DE, FR, AE, AU)
- Parses with python-calamine (fast Excel reader)
- Upserts into `si_keywords` + `si_daily_ranks` tables in Supabase

## Key Tables
- `si_keywords` — keyword master (unique on `marketplace_id, child_asin, keyword`)
- `si_daily_ranks` — daily rank values (unique on `keyword_id, rank_date`)
- `data_imports` — import tracking

## Database
- Supabase project: `yawaopfqkkvdqtsagmng` (same as Chalkola ONE)
- Uses `SUPABASE_SERVICE_KEY` (bypasses RLS)

## Country Codes
| Country | SI Code | Marketplace UUID |
|---------|---------|-----------------|
| US | US | f47ac10b-..d479 |
| CA | CA | a1b2c3d4-..d480 |
| UK | GB | b2c3d4e5-..d481 |
| DE | DE | c3d4e5f6-..d482 |
| FR | FR | d4e5f6a7-..d483 |
| AE | AE | e5f6a7b8-..d484 |
| AU | AU | f6a7b8c9-..d485 |

## Batch Sizes
- Keywords: 500/batch
- Ranks: 2000/batch
- Keyword ID fetch: 10000/page

## Running Locally
```bash
pip install -r requirements.txt
export SI_EMAIL=... SI_PASSWORD=... SUPABASE_URL=... SUPABASE_SERVICE_KEY=...
python scripts/pull_rankings.py --country US --days 7
```
