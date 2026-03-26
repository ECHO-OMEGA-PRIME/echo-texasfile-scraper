-- Schema for echo-texasfile-scraper
-- Auto-generated from source code (src/index.ts)
-- D1 Database: echo-texasfile-scraper (7c50467a-43ef-44c4-bf2b-862e386e0a72)
-- Run: npx wrangler d1 execute echo-texasfile-scraper --remote --file=./schema.sql

DROP TABLE IF EXISTS session_log;
DROP TABLE IF EXISTS enrichment_stats;
DROP TABLE IF EXISTS enrichment_log;

CREATE TABLE enrichment_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id TEXT NOT NULL,
  county TEXT NOT NULL,
  instrument_type TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  legal_description TEXT,
  section TEXT,
  block TEXT,
  survey TEXT,
  abstract_number TEXT,
  lot TEXT,
  subdivision TEXT,
  consideration TEXT,
  grantor TEXT,
  grantee TEXT,
  acres TEXT,
  volume TEXT,
  page TEXT,
  num_pages INTEGER,
  texasfile_instrument_id TEXT,
  raw_response TEXT,
  error_message TEXT,
  duration_ms INTEGER,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE enrichment_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  county TEXT NOT NULL UNIQUE,
  total_records INTEGER DEFAULT 0,
  enriched INTEGER DEFAULT 0,
  failed INTEGER DEFAULT 0,
  skipped INTEGER DEFAULT 0,
  no_data INTEGER DEFAULT 0,
  pending INTEGER DEFAULT 0,
  legal_desc_found INTEGER DEFAULT 0,
  last_enriched_at TEXT,
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE session_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL,
  status TEXT NOT NULL,
  error_message TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_el_doc_id ON enrichment_log(doc_id);
CREATE INDEX IF NOT EXISTS idx_el_county ON enrichment_log(county);
CREATE INDEX IF NOT EXISTS idx_el_status ON enrichment_log(status);
