require('dotenv').config();
const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const tough = require('tough-cookie');
const { wrapper } = require('axios-cookiejar-support');
const axios = wrapper(require('axios'));
const { URL } = require('url');

const PORT = parseInt(process.env.PORT || '3000', 10);
const COOKIES_FILE = process.env.COOKIES_FILE || path.join(__dirname, 'cookies.txt');
const BASE = 'https://www.instagram.com';
const IOS_BASE = 'https://i.instagram.com';
const DEFAULT_X_IG_APP_ID = process.env.X_IG_APP_ID || '1217981644879628';
const DEFAULT_UA = process.env.UA || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36';
const MOBILE_UA = process.env.MOBILE_UA || 'Instagram 219.0.0.12.117 Android (30/11; 420dpi; 1080x2340; samsung; SM-G981B; qcom; en_US)';

const MAX_SESSIONS = parseInt(process.env.MAX_SESSIONS || '8', 10);
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || '20', 10);
const PAGINATION_DELAY_MS = parseInt(process.env.PAGINATION_DELAY_MS || '250', 10);
const FALLBACK_PAGES = parseInt(process.env.FALLBACK_PAGES || '20', 10);
const MAX_PUPPETEER_SCROLLS = parseInt(process.env.MAX_PUPPETEER_SCROLLS || '40', 10);
const PER_SESSION_PAGES = parseInt(process.env.PER_SESSION_PAGES || '4', 10);

const CACHE_DIR = path.join(__dirname, 'cache');
const CACHE_MIN_SIZE = parseInt(process.env.CACHE_MIN_SIZE || '40', 10);
const CACHE_PREFETCH_BATCH = parseInt(process.env.CACHE_PREFETCH_BATCH || '80', 10);
const RECENT_MAX = parseInt(process.env.RECENT_MAX || '300', 10);
const CACHE_TTL_MS = parseInt(process.env.CACHE_TTL_MS || String(1000 * 60 * 60 * 24), 10); // 24h

const DEBUG = String(process.env.DEBUG || 'false').toLowerCase() === 'true';

function log(...args) { if (DEBUG) console.log('[DEBUG]', ...args); }
function ensureDir(d) { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); }
ensureDir(CACHE_DIR);

// If IG_COOKIE env var provided (base64-encoded netscape format cookies.txt), decode and write to COOKIES_FILE.
// This runs at process start and will overwrite the existing cookies file if present.
if (process.env.IG_COOKIE) {
  try {
    const decoded = Buffer.from(process.env.IG_COOKIE, 'base64').toString('utf8');
    if (decoded && decoded.trim()) {
      // optional: keep a small backup of an existing file
      try {
        if (fs.existsSync(COOKIES_FILE)) {
          const bak = `${COOKIES_FILE}.bak.${Date.now()}`;
          fs.copyFileSync(COOKIES_FILE, bak);
          log(`Existing cookies file backed up to ${bak}`);
        }
      } catch (e) {
        // non-fatal
        log('backup cookies file failed', e && e.message ? e.message : e);
      }

      // write file with restrictive permission where possible
      try {
        fs.writeFileSync(COOKIES_FILE, decoded, { mode: 0o600 });
      } catch (e) {
        // fallback if mode unsupported on platform
        fs.writeFileSync(COOKIES_FILE, decoded);
      }

      console.log(`IG_COOKIE detected: wrote cookies to ${COOKIES_FILE}`);
    } else {
      console.error('IG_COOKIE env var present but decoded content is empty');
    }
  } catch (e) {
    console.error('Failed to decode/write IG_COOKIE env var:', e && e.message ? e.message : e);
  }
}

function readJson(filePath) {
  try {
    if (!fs.existsSync(filePath)) return null;
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    console.error('readJson error', e && e.message ? e.message : e);
    return null;
  }
}
function writeJson(filePath, obj) {
  try { fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), 'utf8'); } catch (e) { console.error('writeJson error', e && e.message ? e.message : e); }
}

function parseNetscapeCookies(cookieFileContent) {
  const lines = cookieFileContent.split(/\r?\n/);
  const cookies = [];
  for (const line of lines) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const parts = t.split(/\t/);
    if (parts.length >= 7) {
      const [domain, , pathVal, secure, , name, value] = parts;
      cookies.push({ domain, path: pathVal || '/', name, value, secure: (String(secure).toLowerCase() === 'true' || secure === 'TRUE') });
    }
  }
  return cookies;
}

function buildAxiosInstanceFromCookies(cookies) {
  const jar = new tough.CookieJar();
  for (const c of cookies) {
    const cookieStr = `${c.name}=${c.value}; Domain=${c.domain}; Path=${c.path || '/'};`;
    try {
      jar.setCookieSync(cookieStr, BASE, { ignoreError: true });
      jar.setCookieSync(cookieStr, IOS_BASE, { ignoreError: true });
    } catch (e) {}
  }
  const instance = axios.create({
    baseURL: BASE,
    timeout: 30000,
    jar,
    withCredentials: true,
    headers: {
      'User-Agent': DEFAULT_UA,
      Accept: '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
      Referer: 'https://www.instagram.com/',
      'X-IG-App-ID': DEFAULT_X_IG_APP_ID
    }
  });
  return instance;
}

function buildIosInstanceFromCookies(cookies) {
  const jar = new tough.CookieJar();
  for (const c of cookies) {
    const cookieStr = `${c.name}=${c.value}; Domain=${c.domain}; Path=${c.path || '/'};`;
    try {
      jar.setCookieSync(cookieStr, IOS_BASE, { ignoreError: true });
      jar.setCookieSync(cookieStr, BASE, { ignoreError: true });
    } catch (e) {}
  }
  const inst = axios.create({
    baseURL: IOS_BASE,
    timeout: 30000,
    jar,
    withCredentials: true,
    headers: {
      'User-Agent': MOBILE_UA,
      Accept: '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
      Referer: 'https://www.instagram.com/',
      'X-IG-App-ID': DEFAULT_X_IG_APP_ID
    }
  });
  return inst;
}

async function fetchTopSerp(instance, tag, { searchSessionId, rank_token, next_max_id } = {}) {
  const sid = searchSessionId || uuidv4();
  const params = new URLSearchParams();
  params.append('enable_metadata', 'true');
  params.append('query', `#${tag}`);
  if (sid) params.append('search_session_id', sid);
  if (rank_token) params.append('rank_token', rank_token);
  if (next_max_id) params.append('next_max_id', next_max_id);
  const url = `/api/v1/fbsearch/web/top_serp/?${params.toString()}`;
  return instance.get(url, { headers: { 'X-Requested-With': 'XMLHttpRequest' }, validateStatus: s => s < 500 });
}

function ensureAbsoluteUrl(u) {
  if (!u) return null;
  try {
    if (u.startsWith('//')) return 'https:' + u;
    if (/^https?:\/\//i.test(u)) return u;
    if (u.startsWith('/')) return `${BASE}${u}`;
    return 'https://' + u;
  } catch (e) {
    return u;
  }
}

function chooseBestVideoUrl(media) {
  if (!media) return null;

  const pickBest = (arr) => {
    if (!Array.isArray(arr) || arr.length === 0) return null;
    let best = arr[0];
    for (const v of arr) {
      if ((v.bitrate || v.bandwidth || 0) > (best.bitrate || best.bandwidth || 0)) best = v;
      else if ((v.width || 0) > (best.width || 0)) best = v;
    }
    return best && (best.url || best.src || best[Object.keys(best).find(k => k.includes('url') || k.includes('src'))] || null);
  };

  let candidate = pickBest(media.video_versions) || null;
  if (candidate) return ensureAbsoluteUrl(candidate);

  if (media.original && media.original.video_versions) {
    candidate = pickBest(media.original.video_versions);
    if (candidate) return ensureAbsoluteUrl(candidate);
  }

  const carouselCandidates = media.carousel_media || media.carousel_media_items || media.carousel_items || (media.original && (media.original.carousel_media || media.original.carousel_media_items));
  if (Array.isArray(carouselCandidates) && carouselCandidates.length) {
    for (const child of carouselCandidates) {
      const c = chooseBestVideoUrl(child);
      if (c) return ensureAbsoluteUrl(c);
    }
  }

  const single = media.video_url || media.media_url || media.display_url || (media.image_versions2 && media.image_versions2.candidates && media.image_versions2.candidates[0] && media.image_versions2.candidates[0].url) || null;
  if (single && String(single).includes('.mp4')) return ensureAbsoluteUrl(single);

  return null;
}

function normalizeUrlStripQuery(u) {
  try {
    const o = new URL(u);
    return o.origin + o.pathname;
  } catch (e) {
    return String(u).split('?')[0];
  }
}

function shuffle(arr) { for (let i = arr.length - 1; i > 0; --i) { const j = Math.floor(Math.random() * (i + 1)); [arr[i], arr[j]] = [arr[j], arr[i]]; } return arr; }

// ------------------ Cache file helpers ------------------
function tagSafeName(tag) { return tag.replace(/[^a-z0-9_-]/gi, '_').toLowerCase(); }
function cacheFileForTag(tag) { return path.join(CACHE_DIR, `${tagSafeName(tag)}.json`); }
function recentFileForTag(tag) { return path.join(CACHE_DIR, `${tagSafeName(tag)}.recent.json`); }

function loadTagCache(tag) {
  // Try in-memory first
  const mem = getInMemory(tag);
  if (mem) return { items: mem.slice(), updated_at: Date.now() };

  const file = cacheFileForTag(tag);
  const data = readJson(file);
  if (!data || !Array.isArray(data.items)) return { items: [], updated_at: 0 };
  const now = Date.now();
  const items = data.items.filter(it => it && (!it.ts || (now - it.ts) < CACHE_TTL_MS)).map(it => ({ id: String(it.id), url: it.url, ts: it.ts || now }));
  // refresh in-memory hot cache
  setInMemory(tag, items);
  return { items, updated_at: data.updated_at || 0 };
}
function saveTagCache(tag, items) {
  // items expected to be array of { id, url, ts } (already sanitized)
  const file = cacheFileForTag(tag);
  const sanitized = (items || []).map(it => ({ id: String(it.id), url: it.url, ts: it.ts || Date.now() }));
  writeJson(file, { items: sanitized, updated_at: Date.now() });
  setInMemory(tag, sanitized);
}

function loadRecentSet(tag) { const file = recentFileForTag(tag); const data = readJson(file); if (!data || !Array.isArray(data.recent)) return []; return data.recent; }
function saveRecentSet(tag, arr) { const file = recentFileForTag(tag); writeJson(file, { recent: arr.slice(0, RECENT_MAX) }); }

// ------------------ In-memory hot cache ------------------
const inMemoryCache = new Map(); // tag -> { items: Array, ts }
function getInMemory(tag) {
  const e = inMemoryCache.get(tag);
  if (!e) return null;
  if (Date.now() - e.ts > 60 * 1000) { inMemoryCache.delete(tag); return null; } // 60s TTL
  return e.items;
}
function setInMemory(tag, items) { inMemoryCache.set(tag, { items: items.slice(), ts: Date.now() }); }

// ------------------ Sanitize mergeIntoCache ------------------
function sanitizeForCache(item) {
  if (!item) return null;
  const urlCandidate = item.url || item.video_url || item.media_url || item.src || (typeof item === 'string' ? item : null);
  const url = ensureAbsoluteUrl(urlCandidate);
  const id = String(item.id || item.pk || item.fbid || (url ? normalizeUrlStripQuery(url) : '') || '');
  if (!id || !url) return null;
  return { id, url, ts: Date.now() };
}

function mergeIntoCache(tag, fetched) {
  try {
    if (!Array.isArray(fetched) || fetched.length === 0) return;
    const file = cacheFileForTag(tag);
    const data = readJson(file) || { items: [], updated_at: 0 };
    const current = Array.isArray(data.items) ? data.items : [];
    const existingIds = new Set(current.map(i => String(i.id)));
    const now = Date.now();
    for (const f of fetched) {
      const s = sanitizeForCache(f);
      if (!s) continue;
      if (existingIds.has(s.id)) continue;
      current.push(s);
      existingIds.add(s.id);
    }
    const trimmed = current.slice(-1000);
    writeJson(file, { items: trimmed, updated_at: now });
    setInMemory(tag, trimmed);
  } catch (e) {
    console.error('mergeIntoCache error', e && e.message ? e.message : e);
  }
}

// ------------------ Fast pickFromCache ------------------
function pickFromCache(tag, n, recentSet) {
  const cache = loadTagCache(tag).items || [];
  if (!Array.isArray(cache) || cache.length === 0) return { picked: [], remainingCache: [] };

  const picked = [];
  const pickedIds = new Set();
  const triesLimit = Math.max(50, Math.min(cache.length, n * 12)); // bound the random probing
  let tries = 0;

  while (picked.length < n && tries < triesLimit) {
    tries++;
    const idx = Math.floor(Math.random() * cache.length);
    const it = cache[idx];
    if (!it) continue;
    const id = String(it.id || it.url);
    if (pickedIds.has(id) || (recentSet && recentSet.has(id))) continue;
    picked.push({ id, url: it.url });
    pickedIds.add(id);
  }

  // fallback deterministic fill from start
  if (picked.length < n) {
    for (const it of cache) {
      const id = String(it.id || it.url);
      if (pickedIds.has(id) || (recentSet && recentSet.has(id))) continue;
      picked.push({ id, url: it.url });
      pickedIds.add(id);
      if (picked.length >= n) break;
    }
  }

  return { picked, remainingCache: cache };
}

// ------------------ Fetch helpers (unchanged behavior but robust handling) ------------------
async function fetchTagSections(instanceI, tag, maxPages = FALLBACK_PAGES) {
  const collected = [];
  try {
    for (let page = 1; page <= maxPages; ++page) {
      const body = { include_persistent: 0, surface: 'grid', tab: 'recent', page };
      const resp = await instanceI.post(`/api/v1/tags/${encodeURIComponent(tag)}/sections/`, body, { headers: { 'X-Requested-With': 'XMLHttpRequest', 'Content-Type': 'application/json' }, validateStatus: s => s < 500 }).catch(e => ({ error: e, status: e?.response?.status }));
      if (!resp || resp.error) break;
      const data = resp.data || {};
      const sections = data.sections || data.items || [];
      let any = false;
      for (const sec of sections) {
        const items = (sec.layout_content && sec.layout_content.medias) || (sec.medias || sec.items) || [];
        for (const it of items) {
          if (it && it.media) {
            collected.push(it.media);
            any = true;
          }
        }
      }
      if (!any) break;
      await new Promise(r => setTimeout(r, PAGINATION_DELAY_MS));
    }
  } catch (e) {
    // ignore
    log('fetchTagSections error', e && e.message ? e.message : e);
  }
  return collected;
}

async function puppeteerScrapeTag(tag, desiredCount = 20, maxScrolls = MAX_PUPPETEER_SCROLLS) {
  if (String(process.env.USE_PUPPETEER || 'false').toLowerCase() !== 'true') return [];
  let puppeteer;
  try { puppeteer = require('puppeteer'); } catch (e) { console.error('Puppeteer not installed but USE_PUPPETEER=true. Install puppeteer to enable scrolling fallback.'); return []; }

  let cookieObjects = [];
  try {
    if (fs.existsSync(COOKIES_FILE)) {
      const raw = fs.readFileSync(COOKIES_FILE, 'utf8');
      const cookies = parseNetscapeCookies(raw);
      cookieObjects = cookies.map(c => ({ name: c.name, value: c.value, domain: c.domain.replace(/^\./, ''), path: c.path || '/', httpOnly: false, secure: !!c.secure }));
    }
  } catch (e) {}

  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  const page = await browser.newPage();
  await page.setUserAgent(MOBILE_UA);
  if (cookieObjects.length) {
    try { await page.setCookie(...cookieObjects); } catch (e) {}
  }

  const url = `https://www.instagram.com/explore/tags/${encodeURIComponent(tag)}/`;
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 }).catch(()=>{});
  await page.waitForTimeout(1500);

  const found = new Map();
  let scrolls = 0;
  while ((found.size < desiredCount) && (scrolls < maxScrolls)) {
    const items = await page.evaluate(() => {
      const out = [];
      document.querySelectorAll('article video').forEach(v => { if (v.src) out.push({ type: 'video', url: v.src, id: v.closest('a') ? v.closest('a').href : null }); });
      document.querySelectorAll('article a').forEach(a => { if (a.href) out.push({ type: 'link', url: a.href, id: a.href }); });
      return out;
    });
    for (const it of items) { const key = it.url || it.id; if (!found.has(key)) found.set(key, it.url || it.id); }
    await page.evaluate(() => { window.scrollBy(0, window.innerHeight * 1.5); });
    await page.waitForTimeout(1200 + Math.floor(Math.random() * 800));
    scrolls++;
  }

  const results = [];
  for (const v of found.values()) {
    if (String(v).includes('.mp4')) results.push(v);
    else if (String(v).includes('/p/')) {
      try {
        const postPage = await browser.newPage();
        await postPage.setUserAgent(MOBILE_UA);
        await postPage.goto(v, { waitUntil: 'networkidle2', timeout: 30000 }).catch(()=>{});
        const vid = await postPage.evaluate(() => { const vidEl = document.querySelector('article video'); return vidEl ? vidEl.src : null; });
        await postPage.close();
        if (vid) results.push(vid);
      } catch (e) {}
    }
    if (results.length >= desiredCount) break;
  }

  await browser.close();
  return results;
}

// ------------------ Refill cache from sources (returns minimal fetched array) ------------------
async function refillCacheFromSources(tag, instance, instanceI, cookies, targetCount) {
  const fetched = [];
  const seen = new Set();
  let sessionsFetched = 0;
  let attempts = 0;

  function ingestMediaObjLocal(m) {
    if (!m) return;
    const candidates = [];
    const direct = chooseBestVideoUrl(m);
    if (direct) candidates.push(direct);
    if (m.items && Array.isArray(m.items)) for (const it of m.items) { const c = chooseBestVideoUrl(it); if (c) candidates.push(c); }
    if (m.carousel_media && Array.isArray(m.carousel_media)) for (const ch of m.carousel_media) { const c = chooseBestVideoUrl(ch); if (c) candidates.push(c); }
    if (m.original && m.original.edge_sidecar_to_children && Array.isArray(m.original.edge_sidecar_to_children.edges)) {
      for (const e of m.original.edge_sidecar_to_children.edges) { const node = e.node; const c = chooseBestVideoUrl(node); if (c) candidates.push(c); }
    }
    for (const url of candidates) {
      if (!url) continue;
      const id = (m.pk || m.id || m.fbid) ? String(m.pk || m.id || m.fbid) : normalizeUrlStripQuery(url);
      if (!seen.has(id)) { seen.add(id); fetched.push({ id, url: ensureAbsoluteUrl(url) }); }
    }
  }

  while (fetched.length < targetCount && attempts < MAX_ATTEMPTS && sessionsFetched < MAX_SESSIONS) {
    attempts++;
    const sessionId = uuidv4();
    const rankToken = uuidv4();
    sessionsFetched++;
    let sessionPages = 0;
    let nextMax = null;
    while (fetched.length < targetCount && sessionPages < (PER_SESSION_PAGES * 2)) {
      sessionPages++;
      let resp;
      try { resp = await fetchTopSerp(instance, tag, { searchSessionId: sessionId, rank_token: rankToken, next_max_id: nextMax }); } catch (e) { resp = null; }
      if (!resp || resp.status !== 200 || !resp.data) break;
      const sections = resp.data.media_grid?.sections || resp.data.sections || resp.data.items || [];
      let got = 0;
      for (const sec of sections) {
        const items = (sec.layout_content && sec.layout_content.medias) || (sec.medias || sec.items) || [];
        for (const it of items) {
          const m = it && (it.media || it);
          if (!m) continue;
          ingestMediaObjLocal(m);
          got++;
        }
      }
      if (Array.isArray(resp.data.edge_hashtag_to_media?.edges)) {
        for (const edge of resp.data.edge_hashtag_to_media.edges) { const node = edge.node; if (node) ingestMediaObjLocal(node); }
      }
      nextMax = resp.data.next_max_id || resp.data.media_grid?.next_max_id || resp.data.next_page_info?.end_cursor || resp.data.page_info?.end_cursor || null;
      if (!got) break;
      await new Promise(r => setTimeout(r, PAGINATION_DELAY_MS));
    }
  }

  if (fetched.length < targetCount) {
    try {
      const more = await fetchTagSections(instanceI, tag, Math.ceil(FALLBACK_PAGES / 2));
      for (const m of more) {
        const direct = chooseBestVideoUrl(m);
        if (direct) {
          const id = (m.pk || m.id || m.fbid) ? String(m.pk || m.id || m.fbid) : normalizeUrlStripQuery(direct);
          if (!seen.has(id)) { seen.add(id); fetched.push({ id, url: ensureAbsoluteUrl(direct) }); }
        }
        if (fetched.length >= targetCount) break;
      }
    } catch (e) { log('fetchTagSections fallback error', e && e.message ? e.message : e); }
  }

  if (fetched.length < targetCount && String(process.env.USE_PUPPETEER || 'false').toLowerCase() === 'true') {
    try {
      const scraped = await puppeteerScrapeTag(tag, Math.max(targetCount - fetched.length, 20), MAX_PUPPETEER_SCROLLS);
      for (const s of scraped) {
        const id = normalizeUrlStripQuery(s);
        if (!seen.has(id)) { seen.add(id); fetched.push({ id, url: ensureAbsoluteUrl(s) }); }
        if (fetched.length >= targetCount) break;
      }
    } catch (e) { log('puppeteerScrapeTag error', e && e.message ? e.message : e); }
  }

  // merge only minimal sanitized objects
  try { mergeIntoCache(tag, fetched); } catch (e) { console.error('mergeIntoCache failed', e && e.message ? e.message : e); }

  return fetched;
}

// ----------------- Express app -----------------
const app = express();
app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  const initialTag = req.query.tag ? String(req.query.tag).replace(/"/g, '"') : 'kiraedit';
  const autoplay = String(req.query.autoplay || 'false').toLowerCase() === 'true';
  res.type('html').send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Random IG MP4 player</title>
<style>
  body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial;margin:18px}
  .controls{margin-bottom:12px}
  input[type=text]{padding:6px 8px;width:220px}
  button{padding:6px 8px;margin-left:6px}
  .secondary{opacity:0.9}
  #videos > div{margin:10px 0}
  video{max-width:100%;height:auto;border:1px solid #ddd}
  .small{font-size:12px;color:#666}
  .muted{color:#666}
  .spinner{display:inline-block;width:12px;height:12px;border:2px solid #ccc;border-top-color:#333;border-radius:50%;animation:spin 1s linear infinite}
  @keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<h1>Random Instagram MP4 (uses server cookies)</h1>
<p>Enter a hashtag (without <code>#</code>) and click <strong>Get</strong>. Server will try several strategies and return random MP4 URLs.</p>
<div class="controls">
  <input id="tag" type="text" placeholder="enter tag, e.g. kiraedit" value="${initialTag}" />
  <button id="go">Get</button>
  <button id="many" class="secondary">Get 5</button>
  <button id="refresh" class="secondary" title="Refill results (shuffles)">Shuffle</button>
  <label class="muted" style="margin-left:8px;"><input id="autoplay" type="checkbox" ${autoplay ? 'checked' : ''} /> autoplay</label>
</div>
<div id="info" aria-live="polite"></div>
<div id="videos"></div>
<div class="footer"><div class="small">Tips: keep your <code>cookies.txt</code> updated (sessionid + csrftoken). If server returns nothing, check server logs for blocked/403 responses.</div></div>
<script>
(function(){
  const info = document.getElementById('info');
  const videos = document.getElementById('videos');
  let lastResults = [];
  function showInfo(text,busy){ info.innerHTML = text||''; if(busy) info.innerHTML += ' <span class="spinner" aria-hidden="true"></span>'; }
  function clearVideos(){ videos.innerHTML = ''; }
  function renderVideos(list, autoplayFlag){ clearVideos(); if(!list||!list.length){ showInfo('No videos found.'); return; } showInfo('Found ' + list.length + ' videos (shuffled).'); lastResults = list.slice(); for(const src of list){ const wrapper = document.createElement('div'); const v = document.createElement('video'); v.controls = true; v.playsInline = true; v.preload = 'metadata'; v.src = src; if(autoplayFlag){ v.autoplay = true; v.muted = true; } wrapper.appendChild(v); videos.appendChild(wrapper); } const first = videos.querySelector('video'); if(first) first.scrollIntoView({ behavior:'smooth', block:'center' }); }
  async function fetchVideos(count){ const tag = document.getElementById('tag').value.trim(); if(!tag) return alert('Please enter a tag.'); showInfo('Loading... (calling server)', true); clearVideos(); try{ const resp = await fetch('/api/random-videos?tag=' + encodeURIComponent(tag) + '&count=' + Number(count || 1), { cache: 'no-store' }); if(!resp.ok){ const text = await resp.text(); showInfo('Error: ' + resp.status + ' - ' + (text || resp.statusText)); return; } const data = await resp.json(); const list = (data && data.videos) || []; renderVideos(list, document.getElementById('autoplay').checked); }catch(err){ console.error(err); showInfo('Fetch error: ' + err.message); } }
  document.getElementById('go').addEventListener('click', function(){ fetchVideos(1); });
  document.getElementById('many').addEventListener('click', function(){ fetchVideos(5); });
  document.getElementById('refresh').addEventListener('click', function(){ if(lastResults && lastResults.length){ for(let i = lastResults.length - 1; i > 0; --i){ const j = Math.floor(Math.random() * (i + 1)); [lastResults[i], lastResults[j]] = [lastResults[j], lastResults[i]]; } renderVideos(lastResults, document.getElementById('autoplay').checked); } else { fetchVideos(5); } });
  (function autoMaybe(){ const url = new URL(location.href); if(url.searchParams.get('autoplay') === 'true'){ setTimeout(()=>document.getElementById('go').click(), 300); }})();
})();
</script>
</body>
</html>`);
});

app.get('/api/random-videos', async (req, res) => {
  try {
    const tag = (req.query.tag || '').trim();
    if (!tag) return res.status(400).json({ error: 'tag query param required' });
    let count = parseInt(req.query.count || '1', 10);
    if (isNaN(count) || count < 1) count = 1;
    if (count > 24) count = 24;

    if (!fs.existsSync(COOKIES_FILE)) {
      return res.status(500).json({ error: `cookies file missing at ${COOKIES_FILE}` });
    }

    const raw = fs.readFileSync(COOKIES_FILE, 'utf8');
    const cookies = parseNetscapeCookies(raw);
    const instance = buildAxiosInstanceFromCookies(cookies);
    const instanceI = buildIosInstanceFromCookies(cookies);

    const csrfC = cookies.find(c => ['csrftoken','csrf_token','csrfmiddlewaretoken','CSRFToken'].includes(c.name));
    if (csrfC && csrfC.value) { instance.defaults.headers['X-CSRFToken'] = csrfC.value; instanceI.defaults.headers['X-CSRFToken'] = csrfC.value; }

    const recentArr = loadRecentSet(tag) || [];
    const recentSet = new Set(recentArr);

    // quick pick from cache
    let { picked } = pickFromCache(tag, count, recentSet);

    const cacheState = loadTagCache(tag);
    // If cache is small, launch a background big prefetch and do a small immediate fetch
    if ((!picked || picked.length < count) && cacheState.items.length < CACHE_MIN_SIZE) {
      log('cache small, launching background prefetch for tag', tag);
      // background prefetch (don't await)
      refillCacheFromSources(tag, instance, instanceI, cookies, CACHE_PREFETCH_BATCH)
        .then(r => log(`background prefetch for [${tag}] returned ${r?.length || 0} items`))
        .catch(e => console.error('background prefetch error', e && e.message ? e.message : e));

      // small synchronous fetch to satisfy this request quickly
      const needNow = Math.max(count - (picked ? picked.length : 0), 6);
      try {
        log('doing small synchronous fetch needNow=', needNow);
        const liveFetched = await refillCacheFromSources(tag, instance, instanceI, cookies, needNow);
        for (const f of liveFetched) {
          if (!picked) picked = [];
          if (!picked.find(p => p.id === f.id)) picked.push({ id: f.id, url: f.url });
          if (picked.length >= count) break;
        }
      } catch (e) {
        console.error('small synchronous fetch failed', e && e.message ? e.message : e);
      }
    }

    // try to fill from cache if still short
    if (!picked || picked.length < count) {
      const afterPick = pickFromCache(tag, count, recentSet);
      picked = afterPick.picked;
    }

    // if still short, do a live larger fetch (but bounded)
    if (!picked || picked.length < count) {
      const liveFetched = await refillCacheFromSources(tag, instance, instanceI, cookies, Math.max(count - (picked ? picked.length : 0), 20));
      for (const f of liveFetched) {
        if (!picked) picked = [];
        if (!picked.find(p => p.id === f.id)) { picked.push({ id: f.id, url: f.url }); }
        if (picked.length >= count) break;
      }
    }

    // fallback to all cache items if still nothing
    if (!picked || picked.length < count) {
      const allCache = loadTagCache(tag).items || [];
      for (const it of allCache) {
        const id = it.id || it.url;
        if (!picked.find(p => p.id === id)) { picked.push({ id, url: it.url }); }
        if (picked.length >= count) break;
      }
    }

    // last-ditch: try one direct top_serp call
    if (!picked || picked.length === 0) {
      try {
        const resp = await fetchTopSerp(instance, tag, { searchSessionId: uuidv4() }).catch(() => null);
        if (resp && resp.data) {
          const sections = resp.data.media_grid?.sections || resp.data.sections || resp.data.items || [];
          const found = [];
          for (const sec of sections) {
            const items = (sec.layout_content && sec.layout_content.medias) || (sec.medias || sec.items) || [];
            for (const it of items) {
              const m = it && (it.media || it);
              if (!m) continue;
              const url = chooseBestVideoUrl(m);
              if (url) found.push({ id: (m.pk || m.id || m.fbid) || normalizeUrlStripQuery(url), url: ensureAbsoluteUrl(url) });
            }
          }
          if (found.length) {
            mergeIntoCache(tag, found);
            for (const f of found) { if (!picked.find(p => p.id === f.id)) picked.push({ id: f.id, url: f.url }); if (picked.length >= count) break; }
          }
        }
      } catch (e) { log('direct top_serp fallback error', e && e.message ? e.message : e); }
    }

    const urls = (picked || []).map(p => p.url).filter(Boolean).slice(0, count);
    if (!urls.length) {
      return res.status(204).json({ tag, pagesFetched: 0, totalVideosFound: 0, videos: [], source: 'none' });
    }

    const newRecent = recentArr.slice();
    for (const p of picked) { const id = p.id || normalizeUrlStripQuery(p.url); newRecent.unshift(id); }
    const deduped = [];
    const seen = new Set();
    for (const id of newRecent) { if (!seen.has(id)) { seen.add(id); deduped.push(id); } if (deduped.length >= RECENT_MAX) break; }
    saveRecentSet(tag, deduped);

    res.json({ tag, fetched_at: new Date().toISOString(), pagesFetched: null, totalVideosFound: loadTagCache(tag).items.length, videos: urls, source: 'cache+live' });

  } catch (err) {
    console.error('API error', err && err.message ? err.message : err);
    res.status(500).json({ error: err && err.message ? err.message : String(err) });
  }
});

app.listen(PORT, () => { console.log(`topserp-random-server listening on http://localhost:${PORT}/`); });
