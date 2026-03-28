/**
 * ECHO PRIME — TexasFile.com Authenticated Scraper Worker v1.0.0
 *
 * Enriches ShadowGlass D1 deed_records with full legal descriptions
 * scraped from TexasFile.com's authenticated document detail API.
 *
 * Architecture:
 *   - TexasFile is Django + React SPA
 *   - Login via POST /login/ with CSRF token
 *   - Document detail via /document/api/summary/texas/{county}/{instrument_id}/
 *   - Server.props JSON injection pattern in HTML responses
 *   - ALL counties require authentication (allow_anon_search: false)
 *   - Residential proxy (IPRoyal) for all requests
 *   - Rate-limited to ~30 req/min with exponential backoff
 *
 * Endpoints:
 *   POST /auth/login         — Login to TexasFile, store session in KV
 *   GET  /auth/status        — Check if session is valid
 *   POST /auth/set-cookies   — Manually set session cookies (reCAPTCHA bypass)
 *   POST /enrich/document    — Fetch + extract single document detail
 *   POST /enrich/batch       — Batch enrich multiple documents
 *   POST /enrich/county      — Enrich all "SEE INSTRUMENT" records for a county
 *   GET  /stats              — Enrichment progress per county
 *   GET  /health             — Health check
 */

import { Hono } from 'hono';

// ═══════════════════════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════════════════════

interface Env {
  DB: D1Database;
  SESSION: KVNamespace;
  SHADOWGLASS: Fetcher;
  ECHO_API_KEY: string;
  TEXASFILE_EMAIL: string;
  TEXASFILE_PASSWORD: string;
  PROXY_HOST: string;
  PROXY_PORT: string;
  PROXY_USER: string;
  PROXY_PASS: string;
  TEXASFILE_BASE_URL: string;
  MAX_REQUESTS_PER_MINUTE: string;
  REQUEST_DELAY_MS: string;
  WORKER_VERSION: string;
}

interface SessionData {
  csrftoken: string;
  sessionid: string;
  created_at: string;
  expires_at: string;
  email: string;
}

interface DocumentDetail {
  doc_id: string;
  county: string;
  county_slug: string;
  instrument_id: string;
  instrument_type: string;
  instrument_number: string;
  recorded_date: string;
  instrument_date: string;
  grantor: string;
  grantee: string;
  legal_description: string;
  section: string;
  block: string;
  survey: string;
  abstract_number: string;
  lot: string;
  subdivision: string;
  consideration: string;
  volume: string;
  page: string;
  book: string;
  acres: string;
  num_pages: number;
  raw_data: Record<string, unknown>;
}

interface EnrichmentResult {
  doc_id: string;
  county: string;
  status: 'enriched' | 'failed' | 'skipped' | 'no_data';
  legal_description: string | null;
  section: string | null;
  block: string | null;
  survey: string | null;
  error: string | null;
  duration_ms: number;
}

interface BatchRequest {
  county: string;
  doc_ids: string[];
  limit?: number;
}

interface CountyEnrichRequest {
  county: string;
  limit?: number;
  offset?: number;
  instrument_types?: string[];
}

// ═══════════════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════════════

const SESSION_KV_KEY = 'texasfile_session';
const SESSION_TTL_SECONDS = 45 * 60; // 45 minutes
const RATE_LIMIT_KV_KEY = 'rate_limit_window';
const DEFAULT_MAX_RPM = 30;
const DEFAULT_DELAY_MS = 2000;

const COUNTY_SLUG_MAP: Record<string, string> = {
  'REEVES': 'reeves',
  'ECTOR': 'ector',
  'MIDLAND': 'midland',
  'MARTIN': 'martin',
  'HOWARD': 'howard',
  'ANDREWS': 'andrews',
  'WARD': 'ward',
  'WINKLER': 'winkler',
  'CRANE': 'crane',
  'UPTON': 'upton',
  'LOVING': 'loving',
  'PECOS': 'pecos',
  'GLASSCOCK': 'glasscock',
  'STERLING': 'sterling',
  'REAGAN': 'reagan',
  'IRION': 'irion',
  'CROCKETT': 'crockett',
  'SCHLEICHER': 'schleicher',
  'SUTTON': 'sutton',
  'TERRELL': 'terrell',
  'BREWSTER': 'brewster',
  'JEFF DAVIS': 'jeff-davis',
  'PRESIDIO': 'presidio',
  'CULBERSON': 'culberson',
  'HUDSPETH': 'hudspeth',
  'EL PASO': 'el-paso',
  'HARRIS': 'harris',
  'DALLAS': 'dallas',
  'TARRANT': 'tarrant',
  'BEXAR': 'bexar',
  'TRAVIS': 'travis',
  'DENTON': 'denton',
  'COLLIN': 'collin',
  'WILLIAMSON': 'williamson',
  'LUBBOCK': 'lubbock',
  'TOM GREEN': 'tom-green',
  'TAYLOR': 'taylor',
  'NOLAN': 'nolan',
  'MITCHELL': 'mitchell',
  'SCURRY': 'scurry',
  'DAWSON': 'dawson',
  'GAINES': 'gaines',
  'YOAKUM': 'yoakum',
  'TERRY': 'terry',
  'LYNN': 'lynn',
  'GARZA': 'garza',
  'BORDEN': 'borden',
  'FISHER': 'fisher',
  'STONEWALL': 'stonewall',
  'HASKELL': 'haskell',
  'JONES': 'jones',
  'SHACKELFORD': 'shackelford',
  'STEPHENS': 'stephens',
  'PALO PINTO': 'palo-pinto',
  'PARKER': 'parker',
  'WISE': 'wise',
  'MONTAGUE': 'montague',
  'COOKE': 'cooke',
  'GRAYSON': 'grayson',
  'WEBB': 'webb',
  'NUECES': 'nueces',
  'HIDALGO': 'hidalgo',
  'CAMERON': 'cameron',
  'BRAZORIA': 'brazoria',
  'FORT BEND': 'fort-bend',
  'GALVESTON': 'galveston',
  'JEFFERSON': 'jefferson',
  'ORANGE': 'orange',
  'CHAMBERS': 'chambers',
  'LIBERTY': 'liberty',
  'MONTGOMERY': 'montgomery',
  'WALLER': 'waller',
  'GRIMES': 'grimes',
  'BRAZOS': 'brazos',
  'ROBERTSON': 'robertson',
  'LEA': 'lea',
  'EDDY': 'eddy',
  'CHAVES': 'chaves',
};

// ═══════════════════════════════════════════════════════════════════
// SCHEMA
// ═══════════════════════════════════════════════════════════════════

const ENRICHMENT_SCHEMA = `
CREATE TABLE IF NOT EXISTS enrichment_log (
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
)`;

const ENRICHMENT_INDEXES = [
  'CREATE INDEX IF NOT EXISTS idx_el_doc_id ON enrichment_log(doc_id)',
  'CREATE INDEX IF NOT EXISTS idx_el_county ON enrichment_log(county)',
  'CREATE INDEX IF NOT EXISTS idx_el_status ON enrichment_log(status)',
  'CREATE INDEX IF NOT EXISTS idx_el_county_status ON enrichment_log(county, status)',
  'CREATE UNIQUE INDEX IF NOT EXISTS idx_el_dedup ON enrichment_log(county, doc_id)',
];

const ENRICHMENT_STATS_SCHEMA = `
CREATE TABLE IF NOT EXISTS enrichment_stats (
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
)`;

const SESSION_LOG_SCHEMA = `
CREATE TABLE IF NOT EXISTS session_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL,
  status TEXT NOT NULL,
  error_message TEXT,
  created_at TEXT DEFAULT (datetime('now'))
)`;

async function ensureSchema(db: D1Database): Promise<void> {
  const statements = [
    ENRICHMENT_SCHEMA,
    ENRICHMENT_STATS_SCHEMA,
    SESSION_LOG_SCHEMA,
    ...ENRICHMENT_INDEXES,
  ];
  for (const sql of statements) {
    try {
      await db.prepare(sql).run();
    } catch (e) {
      // Index may already exist — that's fine
      const msg = e instanceof Error ? e.message : String(e);
      if (!msg.includes('already exists')) {
        console.error(`Schema error: ${msg}`);
      }
    }
  }
}

// ═══════════════════════════════════════════════════════════════════
// AUTH HELPERS
// ═══════════════════════════════════════════════════════════════════

function authMiddleware(env: Env): (c: any, next: () => Promise<void>) => Promise<Response | void> {
  return async (c, next) => {
    const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
    if (!apiKey || apiKey !== env.ECHO_API_KEY) {
      return c.json({ error: 'Unauthorized', message: 'Valid X-Echo-API-Key header required' }, 401);
    }
    await next();
  };
}

// ═══════════════════════════════════════════════════════════════════
// TEXASFILE HTTP CLIENT
// ═══════════════════════════════════════════════════════════════════

/**
 * Build proxy URL for IPRoyal residential proxy.
 * Cloudflare Workers cannot use SOCKS or CONNECT proxies natively,
 * so we use the proxy via fetch headers where supported, or
 * construct requests that route through the proxy service.
 *
 * NOTE: Cloudflare Workers run on Cloudflare's edge and do NOT support
 * HTTP CONNECT proxies in the traditional sense. For residential proxy
 * usage, we'll make direct requests and rely on Cloudflare's IP diversity,
 * OR the caller can use the Worker from a machine that routes through
 * the proxy. We include proxy config for future use with Browser Rendering
 * or external proxy gateways.
 */
function getProxyHeaders(env: Env): Record<string, string> {
  // If proxy credentials are set, we can use them with an HTTP proxy gateway
  // For now, we include them as custom headers that a proxy relay could use
  if (env.PROXY_HOST && env.PROXY_USER) {
    return {
      'X-Proxy-Host': `${env.PROXY_HOST}:${env.PROXY_PORT || '12321'}`,
      'X-Proxy-Auth': `${env.PROXY_USER}:${env.PROXY_PASS}`,
    };
  }
  return {};
}

/**
 * Make an authenticated request to TexasFile with session cookies.
 * Includes rate limiting, retry logic, and exponential backoff.
 */
async function texasFileFetch(
  url: string,
  options: RequestInit & { session?: SessionData },
  env: Env,
  retryCount: number = 0
): Promise<Response> {
  const maxRetries = 3;
  const headers: Record<string, string> = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': `${env.TEXASFILE_BASE_URL}/`,
    'Origin': env.TEXASFILE_BASE_URL,
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    ...(options.headers as Record<string, string> || {}),
  };

  // Add session cookies if available
  if (options.session) {
    headers['Cookie'] = `csrftoken=${options.session.csrftoken}; sessionid=${options.session.sessionid}`;
    headers['X-CSRFToken'] = options.session.csrftoken;
  }

  try {
    const response = await fetch(url, {
      ...options,
      headers,
      redirect: 'manual',
    });

    // Handle rate limiting
    if (response.status === 429 || response.status === 503) {
      if (retryCount >= maxRetries) {
        throw new Error(`Rate limited after ${maxRetries} retries: ${response.status}`);
      }
      const backoffMs = Math.min(1000 * Math.pow(2, retryCount) + Math.random() * 1000, 30000);
      console.log(`Rate limited (${response.status}), backing off ${backoffMs}ms (retry ${retryCount + 1}/${maxRetries})`);
      await sleep(backoffMs);
      return texasFileFetch(url, options, env, retryCount + 1);
    }

    // Handle server errors with retry
    if (response.status >= 500 && retryCount < maxRetries) {
      const backoffMs = Math.min(2000 * Math.pow(2, retryCount) + Math.random() * 1000, 30000);
      console.log(`Server error (${response.status}), backing off ${backoffMs}ms (retry ${retryCount + 1}/${maxRetries})`);
      await sleep(backoffMs);
      return texasFileFetch(url, options, env, retryCount + 1);
    }

    return response;
  } catch (error) {
    if (retryCount < maxRetries) {
      const backoffMs = Math.min(3000 * Math.pow(2, retryCount), 30000);
      console.log(`Fetch error, backing off ${backoffMs}ms (retry ${retryCount + 1}/${maxRetries}): ${error}`);
      await sleep(backoffMs);
      return texasFileFetch(url, options, env, retryCount + 1);
    }
    throw error;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ═══════════════════════════════════════════════════════════════════
// RATE LIMITER
// ═══════════════════════════════════════════════════════════════════

class RateLimiter {
  private kv: KVNamespace;
  private maxPerMinute: number;
  private windowKey: string;

  constructor(kv: KVNamespace, maxPerMinute: number) {
    this.kv = kv;
    this.maxPerMinute = maxPerMinute;
    this.windowKey = RATE_LIMIT_KV_KEY;
  }

  async canProceed(): Promise<boolean> {
    const now = Date.now();
    const windowStart = now - 60_000;
    const raw = await this.kv.get(this.windowKey);
    const timestamps: number[] = raw ? JSON.parse(raw) : [];
    const recent = timestamps.filter(t => t > windowStart);
    return recent.length < this.maxPerMinute;
  }

  async recordRequest(): Promise<void> {
    const now = Date.now();
    const windowStart = now - 60_000;
    const raw = await this.kv.get(this.windowKey);
    const timestamps: number[] = raw ? JSON.parse(raw) : [];
    const recent = timestamps.filter(t => t > windowStart);
    recent.push(now);
    await this.kv.put(this.windowKey, JSON.stringify(recent), { expirationTtl: 120 });
  }

  async waitForSlot(): Promise<void> {
    let attempts = 0;
    while (attempts < 120) { // Max 2 minutes wait
      if (await this.canProceed()) {
        await this.recordRequest();
        return;
      }
      await sleep(1000);
      attempts++;
    }
    throw new Error('Rate limiter timeout — could not acquire slot in 2 minutes');
  }
}

// ═══════════════════════════════════════════════════════════════════
// TEXASFILE AUTH
// ═══════════════════════════════════════════════════════════════════

/**
 * Login to TexasFile.com.
 *
 * Flow:
 * 1. GET /?login=beta to get CSRF token from Set-Cookie
 * 2. POST /login/ with csrfmiddlewaretoken, username (email), password, next
 * 3. Extract sessionid from Set-Cookie on redirect
 * 4. Store both cookies in KV with 45min TTL
 */
async function loginToTexasFile(
  email: string,
  password: string,
  env: Env
): Promise<{ success: boolean; session?: SessionData; error?: string }> {
  try {
    // Step 1: Get CSRF token from login page
    console.log(`[AUTH] Fetching CSRF token from ${env.TEXASFILE_BASE_URL}/?login=beta`);
    const loginPageResponse = await texasFileFetch(
      `${env.TEXASFILE_BASE_URL}/?login=beta`,
      { method: 'GET' },
      env
    );

    if (!loginPageResponse.ok && loginPageResponse.status !== 302) {
      return { success: false, error: `Login page returned ${loginPageResponse.status}` };
    }

    // Extract CSRF token from Set-Cookie header
    const setCookieHeaders = loginPageResponse.headers.getAll?.('set-cookie')
      || [loginPageResponse.headers.get('set-cookie') || ''];
    let csrfToken = '';
    for (const cookie of setCookieHeaders) {
      const match = cookie.match(/csrftoken=([^;]+)/);
      if (match) {
        csrfToken = match[1];
        break;
      }
    }

    // Also try extracting from HTML body (Django often embeds it)
    if (!csrfToken) {
      const html = await loginPageResponse.text();
      const htmlMatch = html.match(/csrfmiddlewaretoken.*?value="([^"]+)"/);
      if (htmlMatch) {
        csrfToken = htmlMatch[1];
      }
      // Also check for cookie in meta tag
      const metaMatch = html.match(/name="csrf-token"\s+content="([^"]+)"/);
      if (metaMatch && !csrfToken) {
        csrfToken = metaMatch[1];
      }
    }

    if (!csrfToken) {
      // Try reading from the response body for any csrftoken references
      const bodyText = loginPageResponse.bodyUsed ? '' : await loginPageResponse.text();
      const bodyMatch = bodyText.match(/csrftoken['":\s]+([a-zA-Z0-9]{32,64})/);
      if (bodyMatch) {
        csrfToken = bodyMatch[1];
      }
    }

    if (!csrfToken) {
      return { success: false, error: 'Could not extract CSRF token from login page' };
    }

    console.log(`[AUTH] Got CSRF token: ${csrfToken.substring(0, 8)}...`);

    // Step 2: POST login form
    const formData = new URLSearchParams();
    formData.append('csrfmiddlewaretoken', csrfToken);
    formData.append('username', email);
    formData.append('password', password);
    formData.append('next', '/');

    console.log(`[AUTH] Submitting login for ${email}`);
    const loginResponse = await fetch(`${env.TEXASFILE_BASE_URL}/login/`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': `csrftoken=${csrfToken}`,
        'X-CSRFToken': csrfToken,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Referer': `${env.TEXASFILE_BASE_URL}/?login=beta`,
        'Origin': env.TEXASFILE_BASE_URL,
      },
      redirect: 'manual',
    });

    console.log(`[AUTH] Login response: ${loginResponse.status}`);

    // Extract sessionid from response cookies
    const loginCookies = loginResponse.headers.getAll?.('set-cookie')
      || [loginResponse.headers.get('set-cookie') || ''];
    let sessionId = '';
    let newCsrf = csrfToken;

    for (const cookie of loginCookies) {
      const sessionMatch = cookie.match(/sessionid=([^;]+)/);
      if (sessionMatch) {
        sessionId = sessionMatch[1];
      }
      const csrfMatch = cookie.match(/csrftoken=([^;]+)/);
      if (csrfMatch) {
        newCsrf = csrfMatch[1];
      }
    }

    // A successful login typically returns 302 redirect with sessionid cookie
    if (!sessionId) {
      // Check if login failed — Django returns 200 with error form on failure
      if (loginResponse.status === 200) {
        const body = await loginResponse.text();
        if (body.includes('Please enter a correct') || body.includes('errorlist') || body.includes('recaptcha')) {
          const hasRecaptcha = body.includes('recaptcha') || body.includes('g-recaptcha');
          return {
            success: false,
            error: hasRecaptcha
              ? 'Login requires reCAPTCHA. Use /auth/set-cookies to provide session cookies manually after solving in browser.'
              : 'Invalid email or password',
          };
        }
      }
      return { success: false, error: `No sessionid in response (status: ${loginResponse.status}). May need reCAPTCHA.` };
    }

    // Step 3: Build session data and store in KV
    const now = new Date();
    const expiresAt = new Date(now.getTime() + SESSION_TTL_SECONDS * 1000);
    const session: SessionData = {
      csrftoken: newCsrf,
      sessionid: sessionId,
      created_at: now.toISOString(),
      expires_at: expiresAt.toISOString(),
      email,
    };

    await env.SESSION.put(SESSION_KV_KEY, JSON.stringify(session), {
      expirationTtl: SESSION_TTL_SECONDS,
    });

    console.log(`[AUTH] Login successful. Session stored with ${SESSION_TTL_SECONDS}s TTL.`);

    // Log session creation
    await env.DB.prepare(
      'INSERT INTO session_log (email, status) VALUES (?, ?)'
    ).bind(email, 'success').run();

    return { success: true, session };

  } catch (error) {
    const errMsg = error instanceof Error ? error.message : String(error);
    console.error(`[AUTH] Login failed: ${errMsg}`);

    try {
      await env.DB.prepare(
        'INSERT INTO session_log (email, status, error_message) VALUES (?, ?, ?)'
      ).bind(email, 'failed', errMsg).run();
    } catch (e) { console.warn('[AUTH] Failed to log session failure to D1', { error: (e as Error)?.message || String(e) }); }

    return { success: false, error: errMsg };
  }
}

/**
 * Get the current session from KV, or null if expired/missing.
 */
async function getSession(env: Env): Promise<SessionData | null> {
  const raw = await env.SESSION.get(SESSION_KV_KEY);
  if (!raw) return null;

  const session: SessionData = JSON.parse(raw);
  const now = new Date();
  const expires = new Date(session.expires_at);

  if (now >= expires) {
    await env.SESSION.delete(SESSION_KV_KEY);
    return null;
  }

  return session;
}

/**
 * Validate that a session is still working by hitting a known authenticated page.
 */
async function validateSession(session: SessionData, env: Env): Promise<boolean> {
  try {
    const response = await texasFileFetch(
      `${env.TEXASFILE_BASE_URL}/account/`,
      { method: 'GET', session },
      env
    );
    // If we get redirected to login, session is dead
    const location = response.headers.get('location') || '';
    if (response.status === 302 && (location.includes('login') || location.includes('?login'))) {
      return false;
    }
    // 200 means we're logged in
    return response.status === 200;
  } catch (e) {
    console.warn('[AUTH] Session validation request failed', { error: (e as Error)?.message || String(e) });
    return false;
  }
}

// ═══════════════════════════════════════════════════════════════════
// DOCUMENT DETAIL EXTRACTION
// ═══════════════════════════════════════════════════════════════════

/**
 * Get county slug for TexasFile URL construction.
 */
function getCountySlug(county: string): string {
  const upper = county.toUpperCase().trim();
  if (COUNTY_SLUG_MAP[upper]) {
    return COUNTY_SLUG_MAP[upper];
  }
  // Fallback: lowercase, replace spaces with hyphens
  return county.toLowerCase().trim().replace(/\s+/g, '-');
}

/**
 * Extract Server.props JSON from TexasFile HTML response.
 * TexasFile injects data via: Server.props = {...};
 */
function extractServerProps(html: string): Record<string, unknown> | null {
  // Pattern 1: Server.props = {...};
  const match1 = html.match(/Server\.props\s*=\s*(\{[\s\S]*?\});/);
  if (match1) {
    try {
      return JSON.parse(match1[1]);
    } catch (e) {
      console.warn('[PARSE] Server.props JSON parse failed, attempting fix', { error: (e as Error)?.message || String(e) });
      // Try fixing common issues (trailing commas, etc.)
      try {
        const fixed = match1[1]
          .replace(/,\s*}/g, '}')
          .replace(/,\s*]/g, ']')
          .replace(/'/g, '"');
        return JSON.parse(fixed);
      } catch (e2) { console.warn('[PARSE] Server.props fixed JSON also failed', { error: (e2 as Error)?.message || String(e2) }); }
    }
  }

  // Pattern 2: window.__PRELOADED_STATE__ = {...};
  const match2 = html.match(/window\.__PRELOADED_STATE__\s*=\s*(\{[\s\S]*?\});/);
  if (match2) {
    try {
      return JSON.parse(match2[1]);
    } catch (e) { console.warn('[PARSE] __PRELOADED_STATE__ JSON parse failed', { error: (e as Error)?.message || String(e) }); }
  }

  // Pattern 3: data-props='...' on React mount element
  const match3 = html.match(/data-props='([^']+)'/);
  if (match3) {
    try {
      return JSON.parse(match3[1]);
    } catch (e) { console.warn('[PARSE] data-props JSON parse failed', { error: (e as Error)?.message || String(e) }); }
  }

  // Pattern 4: JSON embedded in script tag with id
  const match4 = html.match(/<script[^>]*id="[^"]*props[^"]*"[^>]*>([\s\S]*?)<\/script>/i);
  if (match4) {
    try {
      return JSON.parse(match4[1]);
    } catch (e) { console.warn('[PARSE] Script tag props JSON parse failed', { error: (e as Error)?.message || String(e) }); }
  }

  return null;
}

/**
 * Extract legal description components from raw document data.
 * Handles multiple formats that TexasFile returns.
 */
function extractLegalDescription(data: Record<string, unknown>): {
  legal_description: string;
  section: string;
  block: string;
  survey: string;
  abstract_number: string;
  lot: string;
  subdivision: string;
  acres: string;
} {
  const result = {
    legal_description: '',
    section: '',
    block: '',
    survey: '',
    abstract_number: '',
    lot: '',
    subdivision: '',
    acres: '',
  };

  // Collect all text fields that might contain legal description info
  const textFields: string[] = [];

  // Direct fields
  const directFields = [
    'legal_description', 'legalDescription', 'legal', 'legal_desc',
    'property_description', 'propertyDescription', 'description',
  ];
  for (const field of directFields) {
    const val = getNestedValue(data, field);
    if (typeof val === 'string' && val.trim().length > 0) {
      textFields.push(val.trim());
    }
  }

  // Check nested structures common in TexasFile
  const summary = data['summary'] || data['document_summary'] || data['documentSummary'];
  if (summary && typeof summary === 'object') {
    const s = summary as Record<string, unknown>;
    for (const field of directFields) {
      const val = s[field];
      if (typeof val === 'string' && val.trim().length > 0) {
        textFields.push(val.trim());
      }
    }
  }

  // Check metadata/details objects
  const meta = data['metadata'] || data['details'] || data['record'] || data['instrument'];
  if (meta && typeof meta === 'object') {
    const m = meta as Record<string, unknown>;
    for (const field of directFields) {
      const val = m[field];
      if (typeof val === 'string' && val.trim().length > 0) {
        textFields.push(val.trim());
      }
    }
  }

  // Check for legal_items or legal_descriptions array
  const legalItems = data['legal_items'] || data['legalItems'] || data['legal_descriptions']
    || (summary && typeof summary === 'object' ? (summary as Record<string, unknown>)['legal_items'] : null);
  if (Array.isArray(legalItems)) {
    for (const item of legalItems) {
      if (typeof item === 'string') {
        textFields.push(item);
      } else if (typeof item === 'object' && item !== null) {
        const i = item as Record<string, unknown>;
        const desc = i['description'] || i['text'] || i['legal_description'] || i['value'];
        if (typeof desc === 'string') {
          textFields.push(desc);
        }
        // Extract structured fields from legal items
        if (i['section'] && typeof i['section'] === 'string') result.section = i['section'];
        if (i['block'] && typeof i['block'] === 'string') result.block = i['block'];
        if (i['survey'] && typeof i['survey'] === 'string') result.survey = i['survey'];
        if (i['abstract'] && typeof i['abstract'] === 'string') result.abstract_number = i['abstract'];
        if (i['abstract_number'] && typeof i['abstract_number'] === 'string') result.abstract_number = i['abstract_number'];
        if (i['lot'] && typeof i['lot'] === 'string') result.lot = i['lot'];
        if (i['subdivision'] && typeof i['subdivision'] === 'string') result.subdivision = i['subdivision'];
        if (i['acres'] && typeof i['acres'] === 'string') result.acres = i['acres'];
        if (i['acreage'] && typeof i['acreage'] === 'string') result.acres = i['acreage'];
      }
    }
  }

  // Combine unique text fields into legal_description
  const uniqueTexts = [...new Set(textFields.filter(t => t.length > 0))];
  result.legal_description = uniqueTexts.join(' | ');

  // Parse section/block/survey from the combined text if not already found
  const fullText = result.legal_description.toUpperCase();

  if (!result.section) {
    const secMatch = fullText.match(/(?:SEC(?:TION)?\.?\s*#?\s*)(\d+[A-Z]?(?:\s*[-&,]\s*\d+[A-Z]?)*)/i);
    if (secMatch) result.section = secMatch[1].trim();
  }

  if (!result.block) {
    const blkMatch = fullText.match(/(?:BL(?:OC)?K\.?\s*#?\s*)([A-Z0-9]+(?:\s*[-&,]\s*[A-Z0-9]+)*)/i);
    if (blkMatch) result.block = blkMatch[1].trim();
  }

  if (!result.survey) {
    // Match common Texas survey patterns
    const surveyPatterns = [
      /(?:SURVEY\.?\s*#?\s*)([A-Z0-9\-]+)/i,
      /((?:T&P|GC&SF|H&TC|EL&RR|TAP|SP|PSL|UP|TCRR)[^,;|]*)/i,
      /(?:ABSTRACT\.?\s*#?\s*)(A-?\d+)/i,
    ];
    for (const pattern of surveyPatterns) {
      const m = fullText.match(pattern);
      if (m) {
        result.survey = m[1].trim();
        break;
      }
    }
  }

  if (!result.abstract_number) {
    const absMatch = fullText.match(/(?:ABST(?:RACT)?\.?\s*#?\s*)(A?-?\d+)/i);
    if (absMatch) result.abstract_number = absMatch[1].trim();
  }

  if (!result.lot) {
    const lotMatch = fullText.match(/(?:LOT\.?\s*#?\s*)(\d+[A-Z]?(?:\s*[-&,]\s*\d+[A-Z]?)*)/i);
    if (lotMatch) result.lot = lotMatch[1].trim();
  }

  if (!result.subdivision) {
    const subMatch = fullText.match(/(?:SUBD(?:IVISION)?\.?\s*:?\s*)([\w\s]+?)(?:\s*(?:LOT|BLK|SEC|,|$))/i);
    if (subMatch) result.subdivision = subMatch[1].trim();
  }

  if (!result.acres) {
    const acreMatch = fullText.match(/([\d,.]+)\s*(?:AC(?:RES?)?|ACRES)/i);
    if (acreMatch) result.acres = acreMatch[1].trim();
  }

  // Check direct named fields in data for section/block/survey
  const structuredFields: Record<string, string[]> = {
    section: ['section', 'sec', 'section_number'],
    block: ['block', 'blk', 'block_number'],
    survey: ['survey', 'survey_name', 'abstract_survey'],
    abstract_number: ['abstract', 'abstract_number', 'abstract_no', 'abs'],
    lot: ['lot', 'lot_number'],
    subdivision: ['subdivision', 'subdiv', 'addition'],
    acres: ['acres', 'acreage', 'area_acres'],
  };

  for (const [resultKey, fieldNames] of Object.entries(structuredFields)) {
    if (result[resultKey as keyof typeof result]) continue;
    for (const source of [data, summary, meta]) {
      if (!source || typeof source !== 'object') continue;
      const s = source as Record<string, unknown>;
      for (const fn of fieldNames) {
        const val = s[fn];
        if (typeof val === 'string' && val.trim().length > 0) {
          (result as Record<string, string>)[resultKey] = val.trim();
          break;
        }
        if (typeof val === 'number') {
          (result as Record<string, string>)[resultKey] = String(val);
          break;
        }
      }
      if (result[resultKey as keyof typeof result]) break;
    }
  }

  return result;
}

/**
 * Get a nested value from an object using dot notation or simple key.
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.');
  let current: unknown = obj;
  for (const part of parts) {
    if (current === null || current === undefined || typeof current !== 'object') return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

/**
 * Extract grantor/grantee from TexasFile document data.
 */
function extractParties(data: Record<string, unknown>): { grantor: string; grantee: string } {
  const result = { grantor: '', grantee: '' };

  // Check direct fields
  const grantorFields = ['grantor', 'grantors', 'seller', 'from_party', 'fromParty', 'conveyor'];
  const granteeFields = ['grantee', 'grantees', 'buyer', 'to_party', 'toParty', 'recipient'];

  for (const source of [data, data['summary'], data['metadata'], data['details'], data['record']]) {
    if (!source || typeof source !== 'object') continue;
    const s = source as Record<string, unknown>;

    for (const field of grantorFields) {
      if (result.grantor) break;
      const val = s[field];
      if (typeof val === 'string' && val.trim()) {
        result.grantor = val.trim();
      } else if (Array.isArray(val)) {
        result.grantor = val
          .map(v => typeof v === 'string' ? v : typeof v === 'object' && v !== null ? (v as Record<string, string>).name || '' : '')
          .filter(Boolean)
          .join('; ');
      }
    }

    for (const field of granteeFields) {
      if (result.grantee) break;
      const val = s[field];
      if (typeof val === 'string' && val.trim()) {
        result.grantee = val.trim();
      } else if (Array.isArray(val)) {
        result.grantee = val
          .map(v => typeof v === 'string' ? v : typeof v === 'object' && v !== null ? (v as Record<string, string>).name || '' : '')
          .filter(Boolean)
          .join('; ');
      }
    }
  }

  // Check parties array pattern
  const parties = data['parties'] || (data['summary'] && typeof data['summary'] === 'object' ? (data['summary'] as Record<string, unknown>)['parties'] : null);
  if (Array.isArray(parties)) {
    const grantors: string[] = [];
    const grantees: string[] = [];
    for (const party of parties) {
      if (typeof party !== 'object' || party === null) continue;
      const p = party as Record<string, unknown>;
      const name = (p.name || p.party_name || '') as string;
      const role = ((p.role || p.party_type || p.type || '') as string).toUpperCase();
      if (!name) continue;
      if (role.includes('GRANTOR') || role.includes('SELLER') || role.includes('FROM') || role === '1') {
        grantors.push(name);
      } else if (role.includes('GRANTEE') || role.includes('BUYER') || role.includes('TO') || role === '2') {
        grantees.push(name);
      }
    }
    if (grantors.length > 0 && !result.grantor) result.grantor = grantors.join('; ');
    if (grantees.length > 0 && !result.grantee) result.grantee = grantees.join('; ');
  }

  return result;
}

/**
 * Extract consideration (dollar amount) from document data.
 */
function extractConsideration(data: Record<string, unknown>): string {
  const fields = ['consideration', 'amount', 'price', 'sale_price', 'purchase_price', 'value'];
  for (const source of [data, data['summary'], data['metadata'], data['details'], data['record']]) {
    if (!source || typeof source !== 'object') continue;
    const s = source as Record<string, unknown>;
    for (const field of fields) {
      const val = s[field];
      if (typeof val === 'string' && val.trim()) return val.trim();
      if (typeof val === 'number') return `$${val.toLocaleString()}`;
    }
  }
  return '';
}

/**
 * Fetch and parse a single document detail from TexasFile.
 */
async function fetchDocumentDetail(
  county: string,
  instrumentId: string,
  session: SessionData,
  env: Env
): Promise<{ success: boolean; detail?: DocumentDetail; error?: string }> {
  const countySlug = getCountySlug(county);
  const startTime = Date.now();

  // Try the API endpoint first (faster, returns JSON)
  const apiUrl = `${env.TEXASFILE_BASE_URL}/document/api/summary/texas/${countySlug}-county/instrument/${instrumentId}/`;
  console.log(`[FETCH] Trying API: ${apiUrl}`);

  try {
    const apiResponse = await texasFileFetch(apiUrl, {
      method: 'GET',
      session,
      headers: {
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
      },
    }, env);

    if (apiResponse.ok) {
      const contentType = apiResponse.headers.get('content-type') || '';
      if (contentType.includes('json')) {
        const jsonData = await apiResponse.json() as Record<string, unknown>;
        return parseDocumentResponse(county, countySlug, instrumentId, jsonData, startTime);
      }
    }

    // If API returns redirect to login, session may be expired
    if (apiResponse.status === 302 || apiResponse.status === 401 || apiResponse.status === 403) {
      return { success: false, error: `Auth failed (${apiResponse.status}) — session may be expired` };
    }

    // Fall through to HTML parsing
    console.log(`[FETCH] API returned ${apiResponse.status}, trying HTML page...`);
  } catch (apiErr) {
    console.log(`[FETCH] API error: ${apiErr}, trying HTML page...`);
  }

  // Try the HTML document page
  const htmlUrl = `${env.TEXASFILE_BASE_URL}/document/texas/${countySlug}-county/county-clerk-records/instrument/${instrumentId}/`;
  console.log(`[FETCH] Trying HTML: ${htmlUrl}`);

  try {
    const htmlResponse = await texasFileFetch(htmlUrl, {
      method: 'GET',
      session,
    }, env);

    if (htmlResponse.status === 302) {
      const location = htmlResponse.headers.get('location') || '';
      if (location.includes('login')) {
        return { success: false, error: 'Session expired — redirected to login' };
      }
    }

    if (!htmlResponse.ok) {
      return { success: false, error: `HTML page returned ${htmlResponse.status}` };
    }

    const html = await htmlResponse.text();

    // Extract Server.props
    const serverProps = extractServerProps(html);
    if (serverProps) {
      return parseDocumentResponse(county, countySlug, instrumentId, serverProps, startTime);
    }

    // Try to extract from meta tags and HTML structure as fallback
    const htmlParsed = parseDocumentFromHtml(html, county, countySlug, instrumentId, startTime);
    if (htmlParsed.success) return htmlParsed;

    return { success: false, error: 'Could not extract data from HTML response' };

  } catch (htmlErr) {
    return { success: false, error: `HTML fetch error: ${htmlErr instanceof Error ? htmlErr.message : String(htmlErr)}` };
  }
}

/**
 * Parse document response data (from API JSON or Server.props) into DocumentDetail.
 */
function parseDocumentResponse(
  county: string,
  countySlug: string,
  instrumentId: string,
  data: Record<string, unknown>,
  startTime: number
): { success: boolean; detail?: DocumentDetail; error?: string } {
  try {
    const legal = extractLegalDescription(data);
    const parties = extractParties(data);
    const consideration = extractConsideration(data);

    // Extract document metadata
    const doc = (data['document'] || data['summary'] || data['record'] || data['instrument'] || data) as Record<string, unknown>;

    const instrumentType = String(doc['instrument_type'] || doc['instrumentType'] || doc['doc_type'] || doc['type'] || '');
    const instrumentNumber = String(doc['instrument_number'] || doc['instrumentNumber'] || doc['doc_number'] || doc['number'] || instrumentId);
    const recordedDate = String(doc['recorded_date'] || doc['recordedDate'] || doc['recording_date'] || doc['filed_date'] || '');
    const instrumentDate = String(doc['instrument_date'] || doc['instrumentDate'] || doc['document_date'] || doc['date'] || '');
    const volume = String(doc['volume'] || doc['vol'] || legal.lot ? '' : '');
    const pageNum = String(doc['page'] || doc['pg'] || '');
    const book = String(doc['book'] || doc['bk'] || '');
    const numPages = Number(doc['num_pages'] || doc['numPages'] || doc['page_count'] || doc['pages'] || 0);

    const detail: DocumentDetail = {
      doc_id: instrumentId,
      county: county.toUpperCase(),
      county_slug: countySlug,
      instrument_id: instrumentId,
      instrument_type: instrumentType,
      instrument_number: instrumentNumber,
      recorded_date: recordedDate,
      instrument_date: instrumentDate,
      grantor: parties.grantor,
      grantee: parties.grantee,
      legal_description: legal.legal_description,
      section: legal.section,
      block: legal.block,
      survey: legal.survey,
      abstract_number: legal.abstract_number,
      lot: legal.lot,
      subdivision: legal.subdivision,
      consideration,
      volume: volume || legal.acres ? '' : '',  // Don't overwrite with empty
      page: pageNum,
      book,
      acres: legal.acres,
      num_pages: numPages,
      raw_data: data,
    };

    // Use actual volume/page from legal or doc
    if (!detail.volume && legal.lot) detail.volume = '';
    if (volume && volume !== 'undefined' && volume !== 'null') detail.volume = volume;
    if (pageNum && pageNum !== 'undefined' && pageNum !== 'null') detail.page = pageNum;
    if (book && book !== 'undefined' && book !== 'null') detail.book = book;

    return { success: true, detail };
  } catch (error) {
    return { success: false, error: `Parse error: ${error instanceof Error ? error.message : String(error)}` };
  }
}

/**
 * Fallback: Parse document from HTML structure when Server.props is not available.
 */
function parseDocumentFromHtml(
  html: string,
  county: string,
  countySlug: string,
  instrumentId: string,
  startTime: number
): { success: boolean; detail?: DocumentDetail; error?: string } {
  // Extract data from common HTML patterns
  const extractField = (label: string): string => {
    // Pattern: <label>Field:</label><span>Value</span> or similar
    const patterns = [
      new RegExp(`${label}[:\\s]*</(?:label|th|td|dt|strong|b)>\\s*<(?:span|td|dd|div)[^>]*>([^<]+)`, 'i'),
      new RegExp(`${label}[:\\s]*([^<\\n]+)`, 'i'),
      new RegExp(`"${label}"[:\\s]*"([^"]+)"`, 'i'),
    ];
    for (const pattern of patterns) {
      const match = html.match(pattern);
      if (match && match[1].trim()) return match[1].trim();
    }
    return '';
  };

  const legalDesc = extractField('Legal Description') || extractField('Legal') || extractField('Property Description');
  const grantor = extractField('Grantor') || extractField('From') || extractField('Seller');
  const grantee = extractField('Grantee') || extractField('To') || extractField('Buyer');
  const consideration = extractField('Consideration') || extractField('Amount');
  const recordedDate = extractField('Recording Date') || extractField('Filed Date') || extractField('Recorded');
  const instrumentType = extractField('Instrument Type') || extractField('Document Type') || extractField('Type');
  const instrumentNumber = extractField('Instrument Number') || extractField('Document Number') || instrumentId;
  const volume = extractField('Volume') || extractField('Vol');
  const page = extractField('Page') || extractField('Pg');
  const book = extractField('Book') || extractField('Bk');

  if (!legalDesc && !grantor && !grantee) {
    return { success: false, error: 'No data extractable from HTML' };
  }

  // Parse legal description components
  const fullText = legalDesc.toUpperCase();
  const section = (fullText.match(/SEC(?:TION)?\.?\s*#?\s*(\d+[A-Z]?)/i) || [])[1] || '';
  const block = (fullText.match(/BL(?:OC)?K\.?\s*#?\s*([A-Z0-9]+)/i) || [])[1] || '';
  const survey = (fullText.match(/((?:T&P|GC&SF|H&TC|EL&RR|TAP|SP|PSL|UP)[^,;|]*)/i) || [])[1] || '';
  const abstract = (fullText.match(/ABST(?:RACT)?\.?\s*#?\s*(A?-?\d+)/i) || [])[1] || '';
  const lot = (fullText.match(/LOT\.?\s*#?\s*(\d+[A-Z]?)/i) || [])[1] || '';
  const subdivision = (fullText.match(/SUBD(?:IVISION)?\.?\s*:?\s*([\w\s]+?)(?:\s*(?:LOT|BLK|SEC|,|$))/i) || [])[1] || '';
  const acres = (fullText.match(/([\d,.]+)\s*(?:AC(?:RES?)?)/i) || [])[1] || '';

  const detail: DocumentDetail = {
    doc_id: instrumentId,
    county: county.toUpperCase(),
    county_slug: countySlug,
    instrument_id: instrumentId,
    instrument_type: instrumentType,
    instrument_number: instrumentNumber,
    recorded_date: recordedDate,
    instrument_date: '',
    grantor,
    grantee,
    legal_description: legalDesc,
    section,
    block,
    survey,
    abstract_number: abstract,
    lot,
    subdivision,
    consideration,
    volume,
    page,
    book,
    acres,
    num_pages: 0,
    raw_data: { source: 'html_extraction' },
  };

  return { success: true, detail };
}

// ═══════════════════════════════════════════════════════════════════
// SHADOWGLASS D1 UPDATE
// ═══════════════════════════════════════════════════════════════════

/**
 * Update a ShadowGlass deed_record with enriched data.
 * Uses the service binding to make a direct D1 query through ShadowGlass.
 *
 * Since we can't directly access ShadowGlass's D1 through a service binding,
 * we'll construct an internal API call that ShadowGlass exposes for D1 updates.
 *
 * Fallback: Use wrangler D1 execute directly via our own D1 reference.
 * Since ShadowGlass uses a different D1, we'll call its /api/d1/exec endpoint.
 */
async function updateShadowGlassRecord(
  detail: DocumentDetail,
  env: Env
): Promise<{ success: boolean; error?: string }> {
  try {
    // ShadowGlass has an internal API for record operations
    // Try calling through the service binding
    const updatePayload = {
      action: 'enrich_record',
      county: detail.county,
      doc_id: detail.doc_id,
      updates: {
        legal_description: detail.legal_description || null,
        section: detail.section || null,
        block: detail.block || null,
        survey: detail.survey || null,
        lot: detail.lot || null,
        subdivision: detail.subdivision || null,
        consideration: detail.consideration || null,
        grantor: detail.grantor || null,
        grantee: detail.grantee || null,
        acres: detail.acres || null,
        volume: detail.volume || null,
        page: detail.page || null,
        book: detail.book || null,
        updated_at: new Date().toISOString(),
      },
    };

    // Try the ShadowGlass internal API first
    try {
      const sgResponse = await env.SHADOWGLASS.fetch(
        new Request('https://internal/api/enrich', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(updatePayload),
        })
      );

      if (sgResponse.ok) {
        return { success: true };
      }
    } catch (e) {
      console.warn('[ENRICH] ShadowGlass service binding call failed, falling back to D1', { error: (e as Error)?.message || String(e) });
    }

    // Fallback: Use ShadowGlass's D1 SQL endpoint
    // ShadowGlass v8 exposes /api/d1/query for authenticated queries
    try {
      const sqlParts: string[] = [];
      const bindValues: (string | null)[] = [];

      if (detail.legal_description) {
        sqlParts.push('legal_description = ?');
        bindValues.push(detail.legal_description);
      }
      if (detail.section) {
        sqlParts.push('section = ?');
        bindValues.push(detail.section);
      }
      if (detail.block) {
        sqlParts.push('block = ?');
        bindValues.push(detail.block);
      }
      if (detail.survey) {
        sqlParts.push('survey = ?');
        bindValues.push(detail.survey);
      }
      if (detail.lot) {
        sqlParts.push('lot = ?');
        bindValues.push(detail.lot);
      }
      if (detail.subdivision) {
        sqlParts.push('subdivision = ?');
        bindValues.push(detail.subdivision);
      }
      if (detail.consideration) {
        sqlParts.push('consideration = ?');
        bindValues.push(detail.consideration);
      }
      if (detail.acres) {
        sqlParts.push('acres = ?');
        bindValues.push(detail.acres);
      }
      if (detail.volume) {
        sqlParts.push('volume = ?');
        bindValues.push(detail.volume);
      }
      if (detail.page) {
        sqlParts.push('page = ?');
        bindValues.push(detail.page);
      }
      if (detail.book) {
        sqlParts.push('book = ?');
        bindValues.push(detail.book);
      }
      if (detail.grantor) {
        sqlParts.push('grantor = ?');
        bindValues.push(detail.grantor);
      }
      if (detail.grantee) {
        sqlParts.push('grantee = ?');
        bindValues.push(detail.grantee);
      }

      sqlParts.push("updated_at = datetime('now')");

      if (sqlParts.length <= 1) {
        return { success: true }; // Nothing to update
      }

      const sql = `UPDATE deed_records SET ${sqlParts.join(', ')} WHERE county = ? AND doc_id = ?`;
      bindValues.push(detail.county.toUpperCase());
      bindValues.push(detail.doc_id);

      // Call ShadowGlass D1 query endpoint
      const sgResponse = await env.SHADOWGLASS.fetch(
        new Request('https://internal/api/d1/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql, binds: bindValues }),
        })
      );

      if (sgResponse.ok) {
        return { success: true };
      }

      // If that doesn't work either, try the general admin endpoint
      const sgAdmin = await env.SHADOWGLASS.fetch(
        new Request('https://internal/admin/d1-exec', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql, params: bindValues }),
        })
      );

      if (sgAdmin.ok) {
        return { success: true };
      }

      return { success: false, error: `ShadowGlass D1 update returned ${sgAdmin.status}` };
    } catch (sqlErr) {
      return { success: false, error: `ShadowGlass D1 error: ${sqlErr instanceof Error ? sqlErr.message : String(sqlErr)}` };
    }
  } catch (error) {
    return { success: false, error: `Update error: ${error instanceof Error ? error.message : String(error)}` };
  }
}

// ═══════════════════════════════════════════════════════════════════
// ENRICHMENT PIPELINE
// ═══════════════════════════════════════════════════════════════════

/**
 * Enrich a single document and track results.
 */
async function enrichDocument(
  county: string,
  docId: string,
  session: SessionData,
  rateLimiter: RateLimiter,
  env: Env
): Promise<EnrichmentResult> {
  const startTime = Date.now();
  const result: EnrichmentResult = {
    doc_id: docId,
    county: county.toUpperCase(),
    status: 'failed',
    legal_description: null,
    section: null,
    block: null,
    survey: null,
    error: null,
    duration_ms: 0,
  };

  try {
    // Check if already enriched
    const existing = await env.DB.prepare(
      'SELECT status FROM enrichment_log WHERE county = ? AND doc_id = ?'
    ).bind(county.toUpperCase(), docId).first();

    if (existing && existing.status === 'enriched') {
      result.status = 'skipped';
      result.error = 'Already enriched';
      result.duration_ms = Date.now() - startTime;
      return result;
    }

    // Wait for rate limit slot
    await rateLimiter.waitForSlot();

    // Fetch document detail
    const fetchResult = await fetchDocumentDetail(county, docId, session, env);

    if (!fetchResult.success || !fetchResult.detail) {
      result.status = 'failed';
      result.error = fetchResult.error || 'Unknown fetch error';
      result.duration_ms = Date.now() - startTime;

      // Log failure
      await logEnrichment(env.DB, result, null);
      return result;
    }

    const detail = fetchResult.detail;

    // Check if we got useful data
    if (!detail.legal_description && !detail.section && !detail.block && !detail.grantor && !detail.grantee) {
      result.status = 'no_data';
      result.error = 'Document exists but contains no extractable legal description or party info';
      result.duration_ms = Date.now() - startTime;
      await logEnrichment(env.DB, result, detail);
      return result;
    }

    // Update ShadowGlass D1
    const updateResult = await updateShadowGlassRecord(detail, env);
    if (!updateResult.success) {
      console.warn(`[ENRICH] ShadowGlass update failed for ${docId}: ${updateResult.error}`);
      // Continue anyway — we still have the data locally
    }

    // Set result
    result.status = 'enriched';
    result.legal_description = detail.legal_description || null;
    result.section = detail.section || null;
    result.block = detail.block || null;
    result.survey = detail.survey || null;
    result.duration_ms = Date.now() - startTime;

    // Log enrichment
    await logEnrichment(env.DB, result, detail);

    return result;

  } catch (error) {
    result.status = 'failed';
    result.error = error instanceof Error ? error.message : String(error);
    result.duration_ms = Date.now() - startTime;
    await logEnrichment(env.DB, result, null);
    return result;
  }
}

/**
 * Log enrichment result to own D1.
 */
async function logEnrichment(
  db: D1Database,
  result: EnrichmentResult,
  detail: DocumentDetail | null
): Promise<void> {
  try {
    await db.prepare(`
      INSERT INTO enrichment_log (
        doc_id, county, instrument_type, status,
        legal_description, section, block, survey, abstract_number,
        lot, subdivision, consideration, grantor, grantee, acres,
        volume, page, num_pages, texasfile_instrument_id,
        raw_response, error_message, duration_ms, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(county, doc_id) DO UPDATE SET
        status = excluded.status,
        legal_description = COALESCE(excluded.legal_description, enrichment_log.legal_description),
        section = COALESCE(excluded.section, enrichment_log.section),
        block = COALESCE(excluded.block, enrichment_log.block),
        survey = COALESCE(excluded.survey, enrichment_log.survey),
        abstract_number = COALESCE(excluded.abstract_number, enrichment_log.abstract_number),
        lot = COALESCE(excluded.lot, enrichment_log.lot),
        subdivision = COALESCE(excluded.subdivision, enrichment_log.subdivision),
        consideration = COALESCE(excluded.consideration, enrichment_log.consideration),
        grantor = COALESCE(excluded.grantor, enrichment_log.grantor),
        grantee = COALESCE(excluded.grantee, enrichment_log.grantee),
        acres = COALESCE(excluded.acres, enrichment_log.acres),
        volume = COALESCE(excluded.volume, enrichment_log.volume),
        page = COALESCE(excluded.page, enrichment_log.page),
        num_pages = COALESCE(excluded.num_pages, enrichment_log.num_pages),
        texasfile_instrument_id = COALESCE(excluded.texasfile_instrument_id, enrichment_log.texasfile_instrument_id),
        raw_response = COALESCE(excluded.raw_response, enrichment_log.raw_response),
        error_message = excluded.error_message,
        duration_ms = excluded.duration_ms,
        updated_at = datetime('now')
    `).bind(
      result.doc_id,
      result.county,
      detail?.instrument_type || null,
      result.status,
      detail?.legal_description || null,
      detail?.section || null,
      detail?.block || null,
      detail?.survey || null,
      detail?.abstract_number || null,
      detail?.lot || null,
      detail?.subdivision || null,
      detail?.consideration || null,
      detail?.grantor || null,
      detail?.grantee || null,
      detail?.acres || null,
      detail?.volume || null,
      detail?.page || null,
      detail?.num_pages || null,
      detail?.instrument_id || null,
      detail?.raw_data ? JSON.stringify(detail.raw_data).substring(0, 4000) : null,
      result.error,
      result.duration_ms,
    ).run();
  } catch (err) {
    console.error(`[LOG] Failed to log enrichment for ${result.doc_id}: ${err}`);
  }
}

/**
 * Update county-level stats after enrichment.
 */
async function updateCountyStats(db: D1Database, county: string): Promise<void> {
  try {
    const stats = await db.prepare(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN status = 'enriched' THEN 1 ELSE 0 END) as enriched,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped,
        SUM(CASE WHEN status = 'no_data' THEN 1 ELSE 0 END) as no_data,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN legal_description IS NOT NULL AND legal_description != '' THEN 1 ELSE 0 END) as legal_found
      FROM enrichment_log WHERE county = ?
    `).bind(county.toUpperCase()).first();

    if (!stats) return;

    await db.prepare(`
      INSERT INTO enrichment_stats (county, total_records, enriched, failed, skipped, no_data, pending, legal_desc_found, last_enriched_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
      ON CONFLICT(county) DO UPDATE SET
        total_records = excluded.total_records,
        enriched = excluded.enriched,
        failed = excluded.failed,
        skipped = excluded.skipped,
        no_data = excluded.no_data,
        pending = excluded.pending,
        legal_desc_found = excluded.legal_desc_found,
        last_enriched_at = datetime('now'),
        updated_at = datetime('now')
    `).bind(
      county.toUpperCase(),
      stats.total || 0,
      stats.enriched || 0,
      stats.failed || 0,
      stats.skipped || 0,
      stats.no_data || 0,
      stats.pending || 0,
      stats.legal_found || 0,
    ).run();
  } catch (err) {
    console.error(`[STATS] Failed to update stats for ${county}: ${err}`);
  }
}

/**
 * Fetch "SEE INSTRUMENT" records from ShadowGlass for a county.
 */
async function fetchSeeInstrumentRecords(
  county: string,
  env: Env,
  limit: number = 100,
  offset: number = 0,
  instrumentTypes?: string[]
): Promise<{ doc_ids: string[]; total: number; error?: string }> {
  try {
    // Query ShadowGlass for records with "SEE INSTRUMENT" or empty legal descriptions
    let whereClause = `county = ? AND (
      legal_description LIKE '%SEE INSTRUMENT%'
      OR legal_description LIKE '%SEE DOCUMENT%'
      OR legal_description LIKE '%SEE DEED%'
      OR legal_description = ''
      OR legal_description IS NULL
    )`;
    const binds: string[] = [county.toUpperCase()];

    if (instrumentTypes && instrumentTypes.length > 0) {
      const placeholders = instrumentTypes.map(() => '?').join(', ');
      whereClause += ` AND instrument_type IN (${placeholders})`;
      binds.push(...instrumentTypes);
    }

    // Get total count
    const countResponse = await env.SHADOWGLASS.fetch(
      new Request('https://internal/api/d1/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: `SELECT COUNT(*) as total FROM deed_records WHERE ${whereClause}`,
          binds,
        }),
      })
    );

    let total = 0;
    if (countResponse.ok) {
      const countData = await countResponse.json() as Record<string, unknown>;
      const results = countData['results'] as Array<Record<string, number>> | undefined;
      if (results && results.length > 0) {
        total = results[0].total || 0;
      }
    }

    // Get doc_ids
    const queryResponse = await env.SHADOWGLASS.fetch(
      new Request('https://internal/api/d1/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: `SELECT doc_id FROM deed_records WHERE ${whereClause} AND doc_id IS NOT NULL AND doc_id != '' ORDER BY recorded_date DESC LIMIT ? OFFSET ?`,
          binds: [...binds, String(limit), String(offset)],
        }),
      })
    );

    if (!queryResponse.ok) {
      // Fallback: Try the records search endpoint
      const searchResponse = await env.SHADOWGLASS.fetch(
        new Request(`https://internal/api/records?county=${encodeURIComponent(county)}&legal_description=SEE+INSTRUMENT&limit=${limit}&offset=${offset}`, {
          method: 'GET',
          headers: { 'Content-Type': 'application/json' },
        })
      );

      if (searchResponse.ok) {
        const searchData = await searchResponse.json() as Record<string, unknown>;
        const records = searchData['records'] as Array<Record<string, string>> | undefined;
        const docIds = (records || []).map(r => r.doc_id).filter(Boolean);
        return { doc_ids: docIds, total: total || docIds.length };
      }

      return { doc_ids: [], total: 0, error: `ShadowGlass query failed: ${queryResponse.status}` };
    }

    const queryData = await queryResponse.json() as Record<string, unknown>;
    const results = queryData['results'] as Array<Record<string, string>> | undefined;
    const docIds = (results || []).map(r => r.doc_id).filter(Boolean);

    return { doc_ids: docIds, total };

  } catch (error) {
    return { doc_ids: [], total: 0, error: `Error querying ShadowGlass: ${error instanceof Error ? error.message : String(error)}` };
  }
}

// ═══════════════════════════════════════════════════════════════════
// HONO APP
// ═══════════════════════════════════════════════════════════════════

const app = new Hono<{ Bindings: Env }>();

// ─── CORS (restricted to known origins) ───────────────────────────
const ALLOWED_ORIGINS = ['https://echo-ept.com', 'https://echo-op.com', 'https://www.echo-ept.com', 'https://www.echo-op.com', 'http://localhost:3000', 'http://localhost:3001'];
function getAllowedOrigin(request: Request): string {
  const origin = request.headers.get('Origin') || '';
  return ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
}

app.use('*', async (c, next) => {
  await next();
  const origin = getAllowedOrigin(c.req.raw);
  c.res.headers.set('Access-Control-Allow-Origin', origin);
  c.res.headers.set('Vary', 'Origin');
  c.res.headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  c.res.headers.set('Access-Control-Allow-Headers', 'Content-Type, X-Echo-API-Key, Authorization');
});

app.options('*', (c) => c.text('', 204));

// ─── HEALTH ────────────────────────────────────────────────────────
app.get('/health', async (c) => {
  const env = c.env;
  let dbOk = false;
  let kvOk = false;
  let sgOk = false;

  try {
    await ensureSchema(env.DB);
    const testResult = await env.DB.prepare('SELECT 1 as test').first();
    dbOk = testResult?.test === 1;
  } catch (e) { console.warn('[HEALTH] D1 database check failed', { error: (e as Error)?.message || String(e) }); }

  try {
    await env.SESSION.get('__health_check');
    kvOk = true;
  } catch (e) { console.warn('[HEALTH] KV session store check failed', { error: (e as Error)?.message || String(e) }); }

  try {
    const sgResp = await env.SHADOWGLASS.fetch(new Request('https://internal/health'));
    sgOk = sgResp.ok;
  } catch (e) { console.warn('[HEALTH] ShadowGlass binding check failed', { error: (e as Error)?.message || String(e) }); }

  const session = await getSession(env);

  return c.json({
    status: dbOk ? 'healthy' : 'degraded',
    version: env.WORKER_VERSION || '1.0.0',
    worker: 'echo-texasfile-scraper',
    timestamp: new Date().toISOString(),
    services: {
      d1: dbOk ? 'connected' : 'error',
      kv: kvOk ? 'connected' : 'error',
      shadowglass: sgOk ? 'connected' : 'error',
    },
    session: session ? {
      active: true,
      email: session.email,
      created_at: session.created_at,
      expires_at: session.expires_at,
      minutes_remaining: Math.max(0, Math.round((new Date(session.expires_at).getTime() - Date.now()) / 60000)),
    } : {
      active: false,
    },
    config: {
      max_requests_per_minute: parseInt(env.MAX_REQUESTS_PER_MINUTE || '30'),
      request_delay_ms: parseInt(env.REQUEST_DELAY_MS || '2000'),
      proxy_configured: !!(env.PROXY_HOST && env.PROXY_USER),
    },
  });
});

// ═══════════════════════════════════════════════════════════════════
// AUTH ENDPOINTS
// ═══════════════════════════════════════════════════════════════════

// ─── POST /auth/login ──────────────────────────────────────────────
app.post('/auth/login', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  let email: string;
  let password: string;

  try {
    const body = await c.req.json() as { email?: string; password?: string };
    email = body.email || env.TEXASFILE_EMAIL;
    password = body.password || env.TEXASFILE_PASSWORD;
  } catch (e) {
    console.warn('[AUTH] Failed to parse request body, using env credentials', { error: (e as Error)?.message || String(e) });
    email = env.TEXASFILE_EMAIL;
    password = env.TEXASFILE_PASSWORD;
  }

  if (!email || !password) {
    return c.json({
      error: 'Missing credentials',
      message: 'Provide {email, password} in body or set TEXASFILE_EMAIL and TEXASFILE_PASSWORD secrets',
    }, 400);
  }

  const result = await loginToTexasFile(email, password, env);

  if (!result.success) {
    return c.json({
      success: false,
      error: result.error,
      hint: result.error?.includes('reCAPTCHA')
        ? 'Login in browser, solve reCAPTCHA, then use POST /auth/set-cookies with {csrftoken, sessionid} from browser cookies'
        : undefined,
    }, result.error?.includes('reCAPTCHA') ? 403 : 401);
  }

  return c.json({
    success: true,
    session: {
      email: result.session!.email,
      created_at: result.session!.created_at,
      expires_at: result.session!.expires_at,
      ttl_minutes: SESSION_TTL_SECONDS / 60,
    },
  });
});

// ─── GET /auth/status ──────────────────────────────────────────────
app.get('/auth/status', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  const session = await getSession(env);
  if (!session) {
    return c.json({
      authenticated: false,
      message: 'No active session. Use POST /auth/login or POST /auth/set-cookies',
    });
  }

  // Validate the session is still working
  const isValid = await validateSession(session, env);

  if (!isValid) {
    await env.SESSION.delete(SESSION_KV_KEY);
    return c.json({
      authenticated: false,
      message: 'Session expired or invalid on TexasFile. Re-authenticate.',
      last_session: {
        email: session.email,
        created_at: session.created_at,
        expired_at: new Date().toISOString(),
      },
    });
  }

  return c.json({
    authenticated: true,
    session: {
      email: session.email,
      created_at: session.created_at,
      expires_at: session.expires_at,
      minutes_remaining: Math.max(0, Math.round((new Date(session.expires_at).getTime() - Date.now()) / 60000)),
    },
  });
});

// ─── POST /auth/set-cookies ────────────────────────────────────────
app.post('/auth/set-cookies', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  const body = await c.req.json() as {
    csrftoken: string;
    sessionid: string;
    email?: string;
    ttl_minutes?: number;
  };

  if (!body.csrftoken || !body.sessionid) {
    return c.json({
      error: 'Missing required fields',
      required: ['csrftoken', 'sessionid'],
      optional: ['email', 'ttl_minutes'],
      how_to_get: 'Login to texasfile.com in browser, open DevTools > Application > Cookies, copy csrftoken and sessionid values',
    }, 400);
  }

  const ttlSeconds = (body.ttl_minutes || 45) * 60;
  const now = new Date();
  const expiresAt = new Date(now.getTime() + ttlSeconds * 1000);

  const session: SessionData = {
    csrftoken: body.csrftoken,
    sessionid: body.sessionid,
    created_at: now.toISOString(),
    expires_at: expiresAt.toISOString(),
    email: body.email || 'manual_session',
  };

  // Validate the cookies work
  const isValid = await validateSession(session, env);
  if (!isValid) {
    return c.json({
      success: false,
      error: 'Provided cookies do not authenticate successfully on TexasFile',
      hint: 'Ensure you copied the correct csrftoken and sessionid values from your browser',
    }, 400);
  }

  await env.SESSION.put(SESSION_KV_KEY, JSON.stringify(session), {
    expirationTtl: ttlSeconds,
  });

  await env.DB.prepare(
    'INSERT INTO session_log (email, status) VALUES (?, ?)'
  ).bind(session.email, 'manual_success').run();

  return c.json({
    success: true,
    session: {
      email: session.email,
      created_at: session.created_at,
      expires_at: session.expires_at,
      ttl_minutes: ttlSeconds / 60,
    },
  });
});

// ═══════════════════════════════════════════════════════════════════
// ENRICHMENT ENDPOINTS
// ═══════════════════════════════════════════════════════════════════

// ─── POST /enrich/document ─────────────────────────────────────────
app.post('/enrich/document', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  const session = await getSession(env);
  if (!session) {
    return c.json({
      error: 'Not authenticated',
      message: 'Use POST /auth/login or POST /auth/set-cookies first',
    }, 401);
  }

  const body = await c.req.json() as {
    county: string;
    doc_id: string;
    instrument_id?: string;
  };

  if (!body.county || !body.doc_id) {
    return c.json({ error: 'Missing required fields: county, doc_id' }, 400);
  }

  const maxRpm = parseInt(env.MAX_REQUESTS_PER_MINUTE || '30');
  const rateLimiter = new RateLimiter(env.SESSION, maxRpm);

  const instrumentId = body.instrument_id || body.doc_id;
  const result = await enrichDocument(body.county, instrumentId, session, rateLimiter, env);

  // Update stats
  await updateCountyStats(env.DB, body.county);

  return c.json({
    success: result.status === 'enriched',
    result,
  });
});

// ─── POST /enrich/batch ────────────────────────────────────────────
app.post('/enrich/batch', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  const session = await getSession(env);
  if (!session) {
    return c.json({
      error: 'Not authenticated',
      message: 'Use POST /auth/login or POST /auth/set-cookies first',
    }, 401);
  }

  const body = await c.req.json() as BatchRequest;

  if (!body.county || !Array.isArray(body.doc_ids) || body.doc_ids.length === 0) {
    return c.json({ error: 'Missing required fields: county, doc_ids (non-empty array)' }, 400);
  }

  const limit = Math.min(body.limit || 50, 200); // Cap at 200 per batch
  const docIds = body.doc_ids.slice(0, limit);
  const maxRpm = parseInt(env.MAX_REQUESTS_PER_MINUTE || '30');
  const delayMs = parseInt(env.REQUEST_DELAY_MS || '2000');
  const rateLimiter = new RateLimiter(env.SESSION, maxRpm);

  const results: EnrichmentResult[] = [];
  let enriched = 0;
  let failed = 0;
  let skipped = 0;
  let noData = 0;
  const startTime = Date.now();

  for (const docId of docIds) {
    // Check session still valid periodically (every 10 docs)
    if (results.length > 0 && results.length % 10 === 0) {
      const stillValid = await validateSession(session, env);
      if (!stillValid) {
        return c.json({
          error: 'Session expired mid-batch',
          results_so_far: results,
          summary: {
            processed: results.length,
            remaining: docIds.length - results.length,
            enriched,
            failed,
            skipped,
            no_data: noData,
          },
        }, 401);
      }
    }

    const result = await enrichDocument(body.county, docId, session, rateLimiter, env);
    results.push(result);

    switch (result.status) {
      case 'enriched': enriched++; break;
      case 'failed': failed++; break;
      case 'skipped': skipped++; break;
      case 'no_data': noData++; break;
    }

    // Delay between requests to avoid detection
    if (docIds.indexOf(docId) < docIds.length - 1) {
      const jitter = Math.random() * 1000;
      await sleep(delayMs + jitter);
    }
  }

  // Update stats
  await updateCountyStats(env.DB, body.county);

  return c.json({
    success: true,
    county: body.county.toUpperCase(),
    summary: {
      total: docIds.length,
      enriched,
      failed,
      skipped,
      no_data: noData,
      duration_ms: Date.now() - startTime,
    },
    results,
  });
});

// ─── POST /enrich/county ───────────────────────────────────────────
app.post('/enrich/county', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  const session = await getSession(env);
  if (!session) {
    return c.json({
      error: 'Not authenticated',
      message: 'Use POST /auth/login or POST /auth/set-cookies first',
    }, 401);
  }

  const body = await c.req.json() as CountyEnrichRequest;

  if (!body.county) {
    return c.json({ error: 'Missing required field: county' }, 400);
  }

  const limit = Math.min(body.limit || 100, 500); // Cap at 500 per request
  const offset = body.offset || 0;

  // Fetch records that need enrichment from ShadowGlass
  const { doc_ids: docIds, total, error: fetchError } = await fetchSeeInstrumentRecords(
    body.county,
    env,
    limit,
    offset,
    body.instrument_types
  );

  if (fetchError) {
    return c.json({
      error: 'Failed to query ShadowGlass',
      details: fetchError,
      hint: 'ShadowGlass service binding may not support direct D1 queries. Try using /enrich/batch with specific doc_ids instead.',
    }, 500);
  }

  if (docIds.length === 0) {
    return c.json({
      success: true,
      county: body.county.toUpperCase(),
      message: 'No records found that need enrichment (no "SEE INSTRUMENT" legal descriptions)',
      total_needing_enrichment: total,
    });
  }

  // Filter out already-enriched docs
  const alreadyEnriched = new Set<string>();
  const batchSize = 50;
  for (let i = 0; i < docIds.length; i += batchSize) {
    const chunk = docIds.slice(i, i + batchSize);
    const placeholders = chunk.map(() => '?').join(',');
    const { results } = await env.DB.prepare(
      `SELECT doc_id FROM enrichment_log WHERE county = ? AND doc_id IN (${placeholders}) AND status = 'enriched'`
    ).bind(body.county.toUpperCase(), ...chunk).all();
    for (const row of results || []) {
      alreadyEnriched.add((row as Record<string, string>).doc_id);
    }
  }

  const toEnrich = docIds.filter(id => !alreadyEnriched.has(id));

  if (toEnrich.length === 0) {
    return c.json({
      success: true,
      county: body.county.toUpperCase(),
      message: `All ${docIds.length} records in this batch already enriched`,
      total_needing_enrichment: total,
      already_enriched: alreadyEnriched.size,
    });
  }

  // Enrich documents
  const maxRpm = parseInt(env.MAX_REQUESTS_PER_MINUTE || '30');
  const delayMs = parseInt(env.REQUEST_DELAY_MS || '2000');
  const rateLimiter = new RateLimiter(env.SESSION, maxRpm);

  const results: EnrichmentResult[] = [];
  let enriched = 0;
  let failed = 0;
  let noData = 0;
  const startTime = Date.now();

  for (const docId of toEnrich) {
    // Session check every 10 docs
    if (results.length > 0 && results.length % 10 === 0) {
      const stillValid = await validateSession(session, env);
      if (!stillValid) {
        await updateCountyStats(env.DB, body.county);
        return c.json({
          error: 'Session expired mid-enrichment',
          county: body.county.toUpperCase(),
          results_so_far: {
            processed: results.length,
            remaining: toEnrich.length - results.length,
            enriched,
            failed,
            no_data: noData,
          },
          hint: 'Re-authenticate and call /enrich/county again — already-enriched records will be skipped',
        }, 401);
      }
    }

    // Check Worker execution time limit (approaching 30s for normal, 15min for cron)
    if (Date.now() - startTime > 25_000) {
      // Approaching time limit — return partial results
      await updateCountyStats(env.DB, body.county);
      return c.json({
        success: true,
        partial: true,
        county: body.county.toUpperCase(),
        message: 'Approaching execution time limit — returning partial results. Call again with offset to continue.',
        summary: {
          processed: results.length,
          remaining: toEnrich.length - results.length,
          total_needing_enrichment: total,
          enriched,
          failed,
          no_data: noData,
          duration_ms: Date.now() - startTime,
          next_offset: offset + results.length + alreadyEnriched.size,
        },
      });
    }

    const result = await enrichDocument(body.county, docId, session, rateLimiter, env);
    results.push(result);

    switch (result.status) {
      case 'enriched': enriched++; break;
      case 'failed': failed++; break;
      case 'no_data': noData++; break;
    }

    // Delay between requests
    if (toEnrich.indexOf(docId) < toEnrich.length - 1) {
      const jitter = Math.random() * 1000;
      await sleep(delayMs + jitter);
    }
  }

  await updateCountyStats(env.DB, body.county);

  return c.json({
    success: true,
    county: body.county.toUpperCase(),
    summary: {
      total_needing_enrichment: total,
      batch_size: docIds.length,
      already_enriched: alreadyEnriched.size,
      processed: toEnrich.length,
      enriched,
      failed,
      no_data: noData,
      duration_ms: Date.now() - startTime,
      next_offset: total > offset + limit ? offset + limit : null,
    },
  });
});

// ═══════════════════════════════════════════════════════════════════
// STATS ENDPOINT
// ═══════════════════════════════════════════════════════════════════

app.get('/stats', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  const county = c.req.query('county');

  // Overall stats
  const overall = await env.DB.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN status = 'enriched' THEN 1 ELSE 0 END) as enriched,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
      SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped,
      SUM(CASE WHEN status = 'no_data' THEN 1 ELSE 0 END) as no_data,
      SUM(CASE WHEN legal_description IS NOT NULL AND legal_description != '' THEN 1 ELSE 0 END) as legal_found,
      AVG(duration_ms) as avg_duration_ms
    FROM enrichment_log
    ${county ? 'WHERE county = ?' : ''}
  `).bind(...(county ? [county.toUpperCase()] : [])).first();

  // Per-county breakdown
  const { results: countyStats } = await env.DB.prepare(`
    SELECT
      county,
      COUNT(*) as total,
      SUM(CASE WHEN status = 'enriched' THEN 1 ELSE 0 END) as enriched,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
      SUM(CASE WHEN status = 'no_data' THEN 1 ELSE 0 END) as no_data,
      SUM(CASE WHEN legal_description IS NOT NULL AND legal_description != '' THEN 1 ELSE 0 END) as legal_found,
      MAX(updated_at) as last_activity
    FROM enrichment_log
    ${county ? 'WHERE county = ?' : ''}
    GROUP BY county
    ORDER BY total DESC
  `).bind(...(county ? [county.toUpperCase()] : [])).all();

  // Recent activity (last 20)
  const { results: recent } = await env.DB.prepare(`
    SELECT doc_id, county, status, legal_description, duration_ms, updated_at
    FROM enrichment_log
    ${county ? 'WHERE county = ?' : ''}
    ORDER BY updated_at DESC
    LIMIT 20
  `).bind(...(county ? [county.toUpperCase()] : [])).all();

  // Session log
  const { results: sessions } = await env.DB.prepare(`
    SELECT email, status, error_message, created_at
    FROM session_log
    ORDER BY created_at DESC
    LIMIT 10
  `).all();

  return c.json({
    overall: {
      total_processed: overall?.total || 0,
      enriched: overall?.enriched || 0,
      failed: overall?.failed || 0,
      skipped: overall?.skipped || 0,
      no_data: overall?.no_data || 0,
      legal_descriptions_found: overall?.legal_found || 0,
      avg_duration_ms: Math.round(Number(overall?.avg_duration_ms || 0)),
      enrichment_rate: overall?.total
        ? `${(((overall?.enriched as number) / (overall?.total as number)) * 100).toFixed(1)}%`
        : '0%',
    },
    counties: countyStats || [],
    recent_activity: (recent || []).map(r => ({
      ...r,
      legal_description: r.legal_description
        ? (r.legal_description as string).substring(0, 100) + ((r.legal_description as string).length > 100 ? '...' : '')
        : null,
    })),
    session_history: sessions || [],
    timestamp: new Date().toISOString(),
  });
});

// ═══════════════════════════════════════════════════════════════════
// UTILITY ENDPOINTS
// ═══════════════════════════════════════════════════════════════════

// ─── GET /counties ─────────────────────────────────────────────────
app.get('/counties', (c) => {
  return c.json({
    counties: Object.entries(COUNTY_SLUG_MAP).map(([name, slug]) => ({
      name,
      slug,
      texasfile_url: `${c.env.TEXASFILE_BASE_URL}/search/texas/${slug}-county/county-clerk-records/`,
    })),
    total: Object.keys(COUNTY_SLUG_MAP).length,
  });
});

// ─── POST /search ──────────────────────────────────────────────────
// Search TexasFile for documents in a county (for finding instrument IDs)
app.post('/search', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  const session = await getSession(env);
  if (!session) {
    return c.json({ error: 'Not authenticated' }, 401);
  }

  const body = await c.req.json() as {
    county: string;
    grantor?: string;
    grantee?: string;
    instrument_number?: string;
    start_date?: string;
    end_date?: string;
    instrument_type?: string;
  };

  if (!body.county) {
    return c.json({ error: 'Missing required field: county' }, 400);
  }

  const countySlug = getCountySlug(body.county);
  const searchUrl = `${env.TEXASFILE_BASE_URL}/search/texas/${countySlug}-county/county-clerk-records/`;

  // Build Django formset search form
  const formData = new URLSearchParams();
  formData.append('csrfmiddlewaretoken', session.csrftoken);

  // TexasFile uses Django formsets for search criteria
  let formIndex = 0;

  if (body.grantor) {
    formData.append(`form-${formIndex}-field`, 'grantor');
    formData.append(`form-${formIndex}-op`, 'contains');
    formData.append(`form-${formIndex}-val`, body.grantor);
    formIndex++;
  }
  if (body.grantee) {
    formData.append(`form-${formIndex}-field`, 'grantee');
    formData.append(`form-${formIndex}-op`, 'contains');
    formData.append(`form-${formIndex}-val`, body.grantee);
    formIndex++;
  }
  if (body.instrument_number) {
    formData.append(`form-${formIndex}-field`, 'instrument_number');
    formData.append(`form-${formIndex}-op`, 'exact');
    formData.append(`form-${formIndex}-val`, body.instrument_number);
    formIndex++;
  }
  if (body.start_date) {
    formData.append(`form-${formIndex}-field`, 'recorded_date');
    formData.append(`form-${formIndex}-op`, 'gte');
    formData.append(`form-${formIndex}-val`, body.start_date);
    formIndex++;
  }
  if (body.end_date) {
    formData.append(`form-${formIndex}-field`, 'recorded_date');
    formData.append(`form-${formIndex}-op`, 'lte');
    formData.append(`form-${formIndex}-val`, body.end_date);
    formIndex++;
  }
  if (body.instrument_type) {
    formData.append(`form-${formIndex}-field`, 'instrument_type');
    formData.append(`form-${formIndex}-op`, 'exact');
    formData.append(`form-${formIndex}-val`, body.instrument_type);
    formIndex++;
  }

  formData.append('form-TOTAL_FORMS', String(formIndex));
  formData.append('form-INITIAL_FORMS', '0');
  formData.append('form-MIN_NUM_FORMS', '0');
  formData.append('form-MAX_NUM_FORMS', '10');

  const maxRpm = parseInt(env.MAX_REQUESTS_PER_MINUTE || '30');
  const rateLimiter = new RateLimiter(env.SESSION, maxRpm);
  await rateLimiter.waitForSlot();

  try {
    const response = await texasFileFetch(searchUrl, {
      method: 'POST',
      session,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: formData.toString(),
    }, env);

    if (!response.ok) {
      return c.json({ error: `Search returned ${response.status}` }, response.status);
    }

    const html = await response.text();
    const serverProps = extractServerProps(html);

    if (serverProps) {
      const searchResults = serverProps['results'] || serverProps['documents'] || serverProps['records'];
      return c.json({
        success: true,
        county: body.county.toUpperCase(),
        results: searchResults,
        total: serverProps['total'] || serverProps['count'] || (Array.isArray(searchResults) ? searchResults.length : 0),
      });
    }

    // Fallback: try to extract result count and instrument IDs from HTML
    const resultCountMatch = html.match(/(\d+)\s*(?:results?|records?|documents?)\s*found/i);
    const instrumentIds: string[] = [];
    const idPattern = /\/instrument\/(\d+)\//g;
    let idMatch;
    while ((idMatch = idPattern.exec(html)) !== null) {
      if (!instrumentIds.includes(idMatch[1])) {
        instrumentIds.push(idMatch[1]);
      }
    }

    return c.json({
      success: true,
      county: body.county.toUpperCase(),
      total: resultCountMatch ? parseInt(resultCountMatch[1]) : instrumentIds.length,
      instrument_ids: instrumentIds,
      note: 'Results extracted from HTML. Use /enrich/document with each instrument_id for full details.',
    });

  } catch (error) {
    return c.json({
      error: 'Search failed',
      details: error instanceof Error ? error.message : String(error),
    }, 500);
  }
});

// ─── GET /enrichment-log ───────────────────────────────────────────
app.get('/enrichment-log', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  const county = c.req.query('county');
  const status = c.req.query('status');
  const limit = Math.min(parseInt(c.req.query('limit') || '50'), 500);
  const offset = parseInt(c.req.query('offset') || '0');

  let where = '1=1';
  const binds: string[] = [];

  if (county) {
    where += ' AND county = ?';
    binds.push(county.toUpperCase());
  }
  if (status) {
    where += ' AND status = ?';
    binds.push(status);
  }

  const { results } = await env.DB.prepare(`
    SELECT id, doc_id, county, instrument_type, status,
           legal_description, section, block, survey, abstract_number,
           grantor, grantee, consideration, acres,
           error_message, duration_ms, created_at, updated_at
    FROM enrichment_log
    WHERE ${where}
    ORDER BY updated_at DESC
    LIMIT ? OFFSET ?
  `).bind(...binds, limit, offset).all();

  const countResult = await env.DB.prepare(`
    SELECT COUNT(*) as total FROM enrichment_log WHERE ${where}
  `).bind(...binds).first();

  return c.json({
    total: countResult?.total || 0,
    limit,
    offset,
    results: (results || []).map(r => ({
      ...r,
      legal_description: r.legal_description
        ? (r.legal_description as string).substring(0, 200)
        : null,
    })),
  });
});

// ─── POST /reset-failed ───────────────────────────────────────────
app.post('/reset-failed', async (c) => {
  const env = c.env;
  const apiKey = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  if (!apiKey || apiKey !== env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  await ensureSchema(env.DB);

  const body = await c.req.json() as { county?: string };
  const county = body.county;

  let result;
  if (county) {
    result = await env.DB.prepare(
      "DELETE FROM enrichment_log WHERE county = ? AND status = 'failed'"
    ).bind(county.toUpperCase()).run();
  } else {
    result = await env.DB.prepare(
      "DELETE FROM enrichment_log WHERE status = 'failed'"
    ).run();
  }

  // Update stats
  if (county) {
    await updateCountyStats(env.DB, county);
  }

  return c.json({
    success: true,
    deleted: result.meta?.changes || 0,
    county: county?.toUpperCase() || 'ALL',
    message: 'Failed records deleted. They will be re-attempted on next enrichment run.',
  });
});

// ─── Catch-all ─────────────────────────────────────────────────────
app.all('*', (c) => {
  return c.json({
    error: 'Not found',
    worker: 'echo-texasfile-scraper',
    version: c.env.WORKER_VERSION || '1.0.0',
    endpoints: {
      'GET /health': 'Health check',
      'POST /auth/login': 'Login to TexasFile (email + password)',
      'GET /auth/status': 'Check session validity',
      'POST /auth/set-cookies': 'Set session cookies manually (after browser reCAPTCHA)',
      'POST /enrich/document': 'Enrich single document {county, doc_id}',
      'POST /enrich/batch': 'Batch enrich {county, doc_ids[], limit?}',
      'POST /enrich/county': 'Enrich all SEE INSTRUMENT records {county, limit?, offset?}',
      'GET /stats': 'Enrichment progress (?county=REEVES)',
      'GET /counties': 'List supported counties with slugs',
      'POST /search': 'Search TexasFile {county, grantor?, grantee?, ...}',
      'GET /enrichment-log': 'View enrichment log (?county=&status=&limit=&offset=)',
      'POST /reset-failed': 'Delete failed records for retry {county?}',
    },
    auth: 'X-Echo-API-Key header required on all endpoints',
  }, 404);
});


app.onError((err, c) => {
  if (err.message?.includes('JSON')) {
    return c.json({ error: 'Invalid JSON body' }, 400);
  }
  console.error(`[echo-texasfile-scraper] ${err.message}`);
  return c.json({ error: 'Internal server error' }, 500);
});

app.notFound((c) => {
  return c.json({ error: 'Not found' }, 404);
});

export default app;
