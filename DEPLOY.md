# echo-texasfile-scraper — Deployment Guide

## Pre-deployment: Create D1 + KV

```bash
# 1. Create D1 database
npx wrangler d1 create echo-texasfile-scraper
# Copy the database_id from output into wrangler.toml

# 2. Create KV namespace
npx wrangler kv namespace create SESSION
# Copy the id from output into wrangler.toml
```

## Deploy

```bash
cd O:\ECHO_OMEGA_PRIME\WORKERS\echo-texasfile-scraper
npx wrangler deploy
```

## Set Secrets

```bash
# Required
echo "echo-omega-prime-forge-x-2026" | npx wrangler secret put ECHO_API_KEY

# TexasFile credentials
echo "your@email.com" | npx wrangler secret put TEXASFILE_EMAIL
echo "yourpassword" | npx wrangler secret put TEXASFILE_PASSWORD

# IPRoyal proxy (optional — Workers use Cloudflare IPs natively)
echo "geo.iproyal.com" | npx wrangler secret put PROXY_HOST
echo "12321" | npx wrangler secret put PROXY_PORT
echo "O3B0q6QDXkKMqY4R" | npx wrangler secret put PROXY_USER
echo "tyn0Gh7FIFvJojWW" | npx wrangler secret put PROXY_PASS
```

## Usage

```bash
BASE="https://echo-texasfile-scraper.bmcii1976.workers.dev"
KEY="echo-omega-prime-forge-x-2026"

# Health check
curl "$BASE/health"

# Login
curl -X POST "$BASE/auth/login" -H "X-Echo-API-Key: $KEY"

# Check session
curl "$BASE/auth/status" -H "X-Echo-API-Key: $KEY"

# Manual cookies (after browser reCAPTCHA)
curl -X POST "$BASE/auth/set-cookies" -H "X-Echo-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"csrftoken":"abc123","sessionid":"xyz789"}'

# Enrich single document
curl -X POST "$BASE/enrich/document" -H "X-Echo-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"county":"REEVES","doc_id":"12345"}'

# Batch enrich
curl -X POST "$BASE/enrich/batch" -H "X-Echo-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"county":"REEVES","doc_ids":["12345","12346","12347"]}'

# Enrich all SEE INSTRUMENT records for county
curl -X POST "$BASE/enrich/county" -H "X-Echo-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"county":"REEVES","limit":100}'

# Stats
curl "$BASE/stats" -H "X-Echo-API-Key: $KEY"
curl "$BASE/stats?county=REEVES" -H "X-Echo-API-Key: $KEY"
```
