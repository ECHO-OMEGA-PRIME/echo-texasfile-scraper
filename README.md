# echo-texasfile-scraper

> Authenticated TexasFile.com document scraper that enriches ShadowGlass deed records with full legal descriptions, section/block/survey data, and grantor/grantee details.

## Overview

Echo TexasFile Scraper is a Cloudflare Worker that logs into TexasFile.com (a Django + React SPA for Texas county records), scrapes document detail pages, and enriches existing deed records in the ShadowGlass D1 database with full legal descriptions. It supports 80+ Texas counties, handles CSRF authentication, implements rate limiting with exponential backoff, and tracks enrichment progress per county.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check with session status and enrichment stats |
| POST | `/auth/login` | Login to TexasFile, store session cookies in KV (45min TTL) |
| GET | `/auth/status` | Check if the current session is valid |
| POST | `/auth/set-cookies` | Manually set session cookies (for reCAPTCHA bypass) |
| POST | `/enrich/document` | Fetch and extract a single document's detail page |
| POST | `/enrich/batch` | Batch enrich multiple documents by doc_id |
| POST | `/enrich/county` | Enrich all "SEE INSTRUMENT" records for a county |
| GET | `/stats` | Enrichment progress per county (enriched/failed/pending counts) |
| GET | `/counties` | List all supported county slugs |
| POST | `/search` | Search enrichment log by county, status, or keyword |
| GET | `/enrichment-log` | View enrichment log entries with pagination |
| POST | `/reset-failed` | Reset failed enrichment records for re-processing |

## Configuration

### Secrets (`wrangler secret put`)

- `ECHO_API_KEY` -- Worker authentication key (required for all endpoints)
- `TEXASFILE_EMAIL` -- TexasFile.com login email
- `TEXASFILE_PASSWORD` -- TexasFile.com login password
- `PROXY_HOST` -- IPRoyal residential proxy host (e.g., `geo.iproyal.com`)
- `PROXY_PORT` -- Proxy port (default: `12321`)
- `PROXY_USER` -- Proxy username
- `PROXY_PASS` -- Proxy password

### D1 Database

| Binding | Database Name | ID |
|---------|---------------|----|
| `DB` | `echo-texasfile-scraper` | `7c50467a-43ef-44c4-bf2b-862e386e0a72` |

**Tables:** `enrichment_log`, `enrichment_stats`, `session_log`

### KV Namespace

| Binding | ID |
|---------|-----|
| `SESSION` | `334bf72b31a84fedbfa5b442558b650e` |

Used for session cookie storage (csrftoken + sessionid) with 45-minute TTL, and rate limit tracking.

### Service Bindings

| Binding | Service |
|---------|---------|
| `SHADOWGLASS` | `shadowglass-v8-warpspeed` |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TEXASFILE_BASE_URL` | `https://www.texasfile.com` | TexasFile base URL |
| `MAX_REQUESTS_PER_MINUTE` | `30` | Rate limit for TexasFile requests |
| `REQUEST_DELAY_MS` | `2000` | Delay between requests in milliseconds |

## Deployment

```bash
cd O:\ECHO_OMEGA_PRIME\WORKERS\echo-texasfile-scraper
npx wrangler deploy

# Set secrets
echo "YOUR_KEY" | npx wrangler secret put ECHO_API_KEY
echo "user@example.com" | npx wrangler secret put TEXASFILE_EMAIL
echo "password" | npx wrangler secret put TEXASFILE_PASSWORD
```

## Architecture

The worker handles TexasFile.com's Django authentication flow:

1. **Login**: GET the login page to extract the CSRF token from `Set-Cookie`, then POST credentials with the CSRF middleware token. Session cookies (csrftoken + sessionid) are stored in KV with a 45-minute TTL.
2. **Document Fetching**: Authenticated requests to `/document/api/summary/texas/{county_slug}/{instrument_id}/` return Server.props JSON embedded in HTML responses.
3. **Data Extraction**: Legal descriptions, section/block/survey, abstract numbers, grantor/grantee, consideration, and acreage are parsed from the JSON response.
4. **Enrichment**: Extracted data is written back to the ShadowGlass D1 database via the service binding and also logged locally in the enrichment tracking tables.

Rate limiting is enforced via a KV-backed sliding window (default 30 req/min). Exponential backoff handles 429/503 responses with up to 3 retries. All requests use browser-like User-Agent headers. The worker supports 80+ Texas county slugs mapped from county names.
