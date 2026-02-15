// topserp-no-cookie-server.js
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const cheerio = require('cheerio');
const { URL } = require('url');

const PORT = parseInt(process.env.PORT || '3000', 10);
const BASE = 'https://www.instagram.com';
const IOS_BASE = 'https://i.instagram.com';
const DEFAULT_X_IG_APP_ID = process.env.X_IG_APP_ID || '1217981644879628';
const DEFAULT_UA = process.env.UA || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36';
const MOBILE_UA = process.env.MOBILE_UA || 'Instagram 219.0.0.12.117 Android (30/11; 420dpi; 1080x2340; samsung; SM-G981B; qcom; en_US)';

const MAX_PUPPETEER_SCROLLS = parseInt(process.env.MAX_PUPPETEER_SCROLLS || '40', 10);
const PAGINATION_DELAY_MS = parseInt(process.env.PAGINATION_DELAY_MS || '250', 10);
const FALLBACK_PAGES = parseInt(process.env.FALLBACK_PAGES || '20', 10);

const CACHE_DIR = path.join(__dirname, 'cache');
const RECENT_MAX = parseInt(process.env.RECENT_MAX || '300', 10);
const DEBUG = String(process.env.DEBUG || 'false').toLowerCase() === 'true';
const DEEP_DEBUG = String(process.env.DEEP_DEBUG || 'true').toLowerCase() === 'true';

function dbg(...args) { if (DEBUG || DEEP_DEBUG) console.log('[DBG]', ...args); }
function dbgDeep(...args) { if (DEEP_DEBUG) console.log('[DEEP]', ...args); }
function ensureDir(d) { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); }
ensureDir(CACHE_DIR);

function readJson(filePath) { try { if (!fs.existsSync(filePath)) return null; return JSON.parse(fs.readFileSync(filePath, 'utf8')); } catch (e) { dbg('readJson error', e && e.message ? e.message : e); return null; } }
function writeJson(filePath, obj) { try { fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), 'utf8'); } catch (e) { dbg('writeJson error', e && e.message ? e.message : e); } }

function ensureAbsoluteUrl(u) {
  if (!u) return null;
  try {
    if (typeof u !== 'string') u = String(u);
    if (u.startsWith('//')) return 'https:' + u;
    if (/^https?:\/\//i.test(u)) return u;
    if (u.startsWith('/')) return `${BASE}${u}`;
    return 'https://' + u;
  } catch (e) { return u; }
}
function normalizeUrlStripQuery(u) { try { const o = new URL(u); return o.origin + o.pathname; } catch (e) { return String(u).split('?')[0]; } }
function tagSafeName(tag) { return tag.replace(/[^a-z0-9_-]/gi, '_').toLowerCase(); }
function cacheFileForTag(tag) { return path.join(CACHE_DIR, `${tagSafeName(tag)}.json`); }
function recentFileForTag(tag) { return path.join(CACHE_DIR, `${tagSafeName(tag)}.recent.json`); }

function loadTagCache(tag) {
  const file = cacheFileForTag(tag);
  const data = readJson(file);
  if (!data || !Array.isArray(data.items)) return { items: [], updated_at: 0 };
  const now = Date.now();
  const items = data.items.filter(it => it && (!it.ts || (now - it.ts) < (1000 * 60 * 60 * 24))).map(it => ({ id: String(it.id), url: it.url, ts: it.ts || now }));
  return { items, updated_at: data.updated_at || 0 };
}
function saveTagCache(tag, items) {
  const file = cacheFileForTag(tag);
  const sanitized = (items || []).map(it => ({ id: String(it.id), url: it.url, ts: it.ts || Date.now() }));
  writeJson(file, { items: sanitized, updated_at: Date.now() });
}
function loadRecentSet(tag) { const file = recentFileForTag(tag); const d = readJson(file); return (d && Array.isArray(d.recent)) ? d.recent : []; }
function saveRecentSet(tag, arr) { const file = recentFileForTag(tag); writeJson(file, { recent: arr.slice(0, RECENT_MAX) }); }

const inMemoryCache = new Map();
function getInMemory(tag) { const e = inMemoryCache.get(tag); if (!e) return null; if (Date.now() - e.ts > 60 * 1000) { inMemoryCache.delete(tag); return null; } return e.items; }
function setInMemory(tag, items) { inMemoryCache.set(tag, { items: items.slice(), ts: Date.now() }); }

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
    for (const f of fetched) {
      const s = sanitizeForCache(f);
      if (!s) continue;
      if (existingIds.has(s.id)) continue;
      current.push(s);
      existingIds.add(s.id);
    }
    const trimmed = current.slice(-1000);
    writeJson(file, { items: trimmed, updated_at: Date.now() });
    setInMemory(tag, trimmed);
  } catch (e) { console.error('mergeIntoCache error', e && e.message ? e.message : e); }
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

function pickFromCache(tag, n, recentSet) {
  const cache = loadTagCache(tag).items || [];
  if (!Array.isArray(cache) || cache.length === 0) return { picked: [], remainingCache: [] };
  const picked = [];
  const pickedIds = new Set();
  const triesLimit = Math.max(50, Math.min(cache.length, n * 12));
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

// ------------------ No-cookie axios instances ------------------
function buildNoCookieInstance() {
  return axios.create({
    baseURL: BASE,
    timeout: 30000,
    headers: {
      'User-Agent': DEFAULT_UA,
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      Referer: 'https://www.instagram.com/',
      'X-IG-App-ID': DEFAULT_X_IG_APP_ID
    }
  });
}
function buildNoCookieIosInstance() {
  return axios.create({
    baseURL: IOS_BASE,
    timeout: 30000,
    headers: {
      'User-Agent': MOBILE_UA,
      Accept: '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
      Referer: 'https://www.instagram.com/',
      'X-IG-App-ID': DEFAULT_X_IG_APP_ID
    }
  });
}

// ------------------ Fetch helpers (no-cookie) ------------------
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
  } catch (e) { dbg('fetchTagSections error', e && e.message ? e.message : e); }
  return collected;
}

// ------------------ Puppeteer fallback (optional) ------------------
async function puppeteerScrapeTag(tag, desiredCount = 20, maxScrolls = MAX_PUPPETEER_SCROLLS) {
  if (String(process.env.USE_PUPPETEER || 'false').toLowerCase() !== 'true') return [];
  let puppeteer;
  try { puppeteer = require('puppeteer'); } catch (e) { console.error('Puppeteer not installed but USE_PUPPETEER=true. Install puppeteer to enable.'); return []; }

  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  const page = await browser.newPage();
  await page.setUserAgent(MOBILE_UA);

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
      } catch (e) { dbg('puppeteer per-post error', e && e.message ? e.message : e); }
    }
    if (results.length >= desiredCount) break;
  }

  await browser.close();
  return results;
}

// ------------------ Refill cache using public endpoints (no-cookie) ------------------
async function refillCacheFromSources(tag, instance, instanceI, targetCount) {
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

  while (fetched.length < targetCount && attempts < 8 && sessionsFetched < 6) {
    attempts++;
    const sessionId = uuidv4();
    const rankToken = uuidv4();
    sessionsFetched++;
    let sessionPages = 0;
    let nextMax = null;
    while (fetched.length < targetCount && sessionPages < 8) {
      sessionPages++;
      let resp = null;
      try { resp = await fetchTopSerp(instance, tag, { searchSessionId: sessionId, rank_token: rankToken, next_max_id: nextMax }); } catch (e) { resp = null; dbg('top_serp error', e && e.message ? e.message : e); }
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
    } catch (e) { dbg('fetchTagSections fallback error', e && e.message ? e.message : e); }
  }

  if (fetched.length < targetCount && String(process.env.USE_PUPPETEER || 'false').toLowerCase() === 'true') {
    try {
      const scraped = await puppeteerScrapeTag(tag, Math.max(targetCount - fetched.length, 20), MAX_PUPPETEER_SCROLLS);
      for (const s of scraped) {
        const id = normalizeUrlStripQuery(s);
        if (!seen.has(id)) { seen.add(id); fetched.push({ id, url: ensureAbsoluteUrl(s) }); }
        if (fetched.length >= targetCount) break;
      }
    } catch (e) { dbg('puppeteerScrapeTag error', e && e.message ? e.message : e); }
  }

  try { mergeIntoCache(tag, fetched); } catch (e) { console.error('mergeIntoCache failed', e && e.message ? e.message : e); }
  return fetched;
}

// ------------------ Aggressive HTML extraction (backup) ------------------
function extractFromHtmlAggressive(html, OUT_set, debugPush) {
  const foundCodes = new Set();
  try {
    const $ = cheerio.load(html || '');
    $('a[href]').each((i, a) => {
      try { const href = $(a).attr('href') || ''; const m = href.match(/^\/(p|reel|tv)\/([^\/?#]+)\/?/i); if (m && m[2]) foundCodes.add(m[2]); } catch(e){}
    });
    $('video').each((i, v) => {
      try { const src = $(v).attr('src') || $(v).attr('data-src') || $(v).attr('data-video-src'); if (src && String(src).includes('.mp4')) OUT_set.add(ensureAbsoluteUrl(src)); } catch(e){}
    });
    const keywords = ['edge_hashtag_to_media', 'shortcode_media', 'shortcode', 'graphql', 'entry_data', 'video_url', 'display_url','og:video'];
    $('script').each((i, s) => {
      try {
        const txt = $(s).html() || '';
        if (!txt || txt.length < 10) return;
        const lower = txt.toLowerCase();
        let matched = false;
        for (const kw of keywords) { if (lower.includes(kw)) { matched = true; break; } }
        if (!matched) return;
        let m;
        const reShort = /"shortcode"\s*:\s*"([A-Za-z0-9_-]+)"/g;
        let foundAny = false;
        while ((m = reShort.exec(txt)) !== null) { foundCodes.add(m[1]); foundAny = true; }
        const reVideo = /"(?:video_url|video_url_sd|display_url)"\s*:\s*"([^"]+\.mp4[^"]*)"/g;
        let mv;
        while ((mv = reVideo.exec(txt)) !== null) { OUT_set.add(ensureAbsoluteUrl(mv[1].replace(/\\u0025/g, '%'))); foundAny = true; }
        if (!foundAny) {
          const pos = txt.search(/shortcode|video_url|edge_hashtag_to_media|graphql/i);
          if (pos !== -1) {
            // try to find JSON around pos
            let start = txt.lastIndexOf('{', pos);
            while (start !== -1) {
              let depth = 0;
              let i2 = start;
              for (; i2 < txt.length; ++i2) {
                const ch = txt[i2];
                if (ch === '{') depth++;
                else if (ch === '}') { depth--; if (depth === 0) break; }
              }
              if (i2 > start) {
                const candidate = txt.slice(start, i2 + 1);
                try {
                  const parsed = JSON.parse(candidate);
                  const sstr = JSON.stringify(parsed);
                  const reV = /https?:\/\/[^"']+\.mp4[^"']*/g;
                  let mm;
                  while ((mm = reV.exec(sstr)) !== null) OUT_set.add(ensureAbsoluteUrl(mm[0]));
                } catch (e) { /* ignore parse fail */ }
              }
              start = txt.lastIndexOf('{', start - 1);
            }
          }
        }
      } catch (e) {}
    });
    const globalRe = /\/(p|reel|tv)\/([A-Za-z0-9_-]{5,})\/?/g;
    let gm;
    while ((gm = globalRe.exec(html || '')) !== null) foundCodes.add(gm[2]);
  } catch (e) {}
  return Array.from(foundCodes);
}

// ------------------ Express app & endpoints ------------------
const app = express();
app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  const rawTag = (req.query.tag ? String(req.query.tag) : 'kiraedit');
  const initialTag = (rawTag).replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const autoplay = String(req.query.autoplay || 'false').toLowerCase() === 'true';
  res.type('html').send(`<!doctype html><html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Random IG MP4 (no-login)</title><style>body{font-family:system-ui,Segoe UI,Roboto,Arial;margin:18px;max-width:900px}input[type=text]{padding:8px;width:260px}button{padding:8px 10px;margin-left:8px}.muted{color:#666;font-size:13px}pre{background:#f7f7f7;padding:12px;border:1px solid #eee;white-space:pre-wrap;max-height:360px;overflow:auto}video{max-width:100%;height:auto;margin:8px 0;border:1px solid #ddd}</style></head><body><h1>Random Instagram MP4 (no-login)</h1><p>Enter hashtag (no <code>#</code>) and press <strong>Get</strong>.</p><div><input id="tag" type="text" placeholder="kiraedit" value="${initialTag}"/><input id="count" type="number" value="3" min="1" max="12" style="width:84px;padding:8px"/><label style="margin-left:8px"><input id="autoplay" type="checkbox"${autoplay ? ' checked' : ''}/> autoplay</label><button id="go">Get</button><button id="debugHtml">Get raw HTML (debug)</button></div><div id="info" style="margin-top:12px" class="muted"></div><div id="videos"></div><h3>Debug / Trace</h3><pre id="debug">DEEP_DEBUG logs will appear here (if enabled on server)</pre><script>(async function(){const info=document.getElementById('info');const videos=document.getElementById('videos');const debugEl=document.getElementById('debug');function showInfo(t){info.textContent=t||'';}function clearVideos(){videos.innerHTML='';}function render(list,autoplay){clearVideos();if(!list||!list.length){showInfo('No videos found.');return;}showInfo('Found '+list.length+' video(s).');for(const src of list){const v=document.createElement('video');v.controls=true;v.playsInline=true;v.preload='metadata';v.src=src;if(autoplay){v.autoplay=true;v.muted=true;}videos.appendChild(v);}const first=videos.querySelector('video');if(first)first.scrollIntoView({behavior:'smooth',block:'center'});}async function doFetch(){const tag=document.getElementById('tag').value.trim();const count=Number(document.getElementById('count').value||1);if(!tag)return alert('enter tag');showInfo('Fetching...');clearVideos();debugEl.textContent='';try{const resp=await fetch('/api/random-videos?tag='+encodeURIComponent(tag)+'&count='+count);const data=await resp.json();const arr=(data&&data.videos)?data.videos:[];render(arr,document.getElementById('autoplay').checked);if(data.debug)debugEl.textContent=data.debug.join('\\n\\n');else debugEl.textContent=JSON.stringify(data,null,2);}catch(e){showInfo('Error: '+e.message);debugEl.textContent=e.message;}}async function getRawHtml(){const tag=document.getElementById('tag').value.trim();if(!tag)return alert('enter tag');showInfo('Fetching raw HTML...');try{const resp=await fetch('/api/debug-html?tag='+encodeURIComponent(tag));const txt=await resp.text();document.getElementById('debug').textContent=txt.slice(0,200000)+'\\n\\n[truncated]';}catch(e){document.getElementById('debug').textContent=e.message;}}document.getElementById('go').addEventListener('click',doFetch);document.getElementById('debugHtml').addEventListener('click',getRawHtml);})();</script></body></html>`);
});

app.get('/api/debug-html', async (req, res) => {
  try {
    const rawTag = (req.query.tag || '').trim();
    if (!rawTag) return res.status(400).send('tag query param required');
    if (!/^[a-zA-Z0-9_-]{1,80}$/.test(rawTag)) return res.status(400).send('invalid tag');
    const client = buildNoCookieInstance();
    const r = await client.get(`/explore/tags/${encodeURIComponent(rawTag)}/`, { validateStatus: s => s < 500 });
    res.type('html').send(String(r.data || ''));
  } catch (e) { res.status(500).send('error: ' + (e && e.message ? e.message : String(e))); }
});

app.get('/api/random-videos', async (req, res) => {
  try {
    const rawTag = (req.query.tag || '').trim();
    if (!rawTag) return res.status(400).json({ error: 'tag query param required' });
    if (!/^[a-zA-Z0-9_-]{1,80}$/.test(rawTag)) return res.status(400).json({ error: 'invalid tag (only letters, numbers, _ and - allowed, max 80 chars)' });
    const tag = rawTag;
    let count = parseInt(req.query.count || '1', 10);
    if (isNaN(count) || count < 1) count = 1;
    if (count > 24) count = 24;

    // Always use no-cookie instances (no login)
    const instance = buildNoCookieInstance();
    const instanceI = buildNoCookieIosInstance();

    const recentArr = loadRecentSet(tag) || [];
    const recentSet = new Set(recentArr);

    // quick pick from cache
    let { picked } = pickFromCache(tag, count, recentSet);

    const cacheState = loadTagCache(tag);
    if ((!picked || picked.length < count) && cacheState.items.length < 40) {
      dbg('cache small, launching background prefetch for tag', tag);
      // background prefetch
      refillCacheFromSources(tag, instance, instanceI, 80).then(r => dbg(`background prefetch for [${tag}] returned ${r?.length || 0} items`)).catch(e => dbg('background prefetch error', e && e.message ? e.message : e));

      const needNow = Math.max(count - (picked ? picked.length : 0), 6);
      try {
        dbg('doing small synchronous fetch needNow=', needNow);
        const liveFetched = await refillCacheFromSources(tag, instance, instanceI, needNow);
        for (const f of liveFetched) {
          if (!picked) picked = [];
          if (!picked.find(p => p.id === f.id)) picked.push({ id: f.id, url: f.url });
          if (picked.length >= count) break;
        }
      } catch (e) { dbg('small synchronous fetch failed', e && e.message ? e.message : e); }
    }

    if (!picked || picked.length < count) {
      const afterPick = pickFromCache(tag, count, recentSet);
      picked = afterPick.picked;
    }

    if (!picked || picked.length < count) {
      const liveFetched = await refillCacheFromSources(tag, instance, instanceI, Math.max(count - (picked ? picked.length : 0), 20));
      for (const f of liveFetched) {
        if (!picked) picked = [];
        if (!picked.find(p => p.id === f.id)) { picked.push({ id: f.id, url: f.url }); }
        if (picked.length >= count) break;
      }
    }

    if (!picked || picked.length < count) {
      const allCache = loadTagCache(tag).items || [];
      for (const it of allCache) {
        const id = it.id || it.url;
        if (!picked.find(p => p.id === id)) { picked.push({ id, url: it.url }); }
        if (picked.length >= count) break;
      }
    }

    // last-ditch direct top_serp call (quick)
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
      } catch (e) { dbg('direct top_serp fallback error', e && e.message ? e.message : e); }
    }

    const urls = (picked || []).map(p => p.url).filter(Boolean).slice(0, count);
    if (!urls.length) {
      // try aggressive HTML parse as final fallback
      try {
        const htmlResp = await instance.get(`/explore/tags/${encodeURIComponent(tag)}/`, { validateStatus: s => s < 500 });
        const OUT = new Set();
        const foundCodes = extractFromHtmlAggressive(String(htmlResp.data || ''), OUT, (k,v)=>dbgDeep(k,v));
        // attempt per-post fetch for first N shortcodes
        for (const sc of foundCodes.slice ? foundCodes.slice(0, 8) : foundCodes) {
          try {
            const r = await instance.get(`/p/${sc}/`, { validateStatus: s => s < 500 }).catch(()=>null);
            const html = String(r && r.data ? r.data : '');
            const og = html.match(/<meta[^>]+property=["']og:video["'][^>]+content=["']([^"']+)["']/i);
            if (og && og[1]) OUT.add(ensureAbsoluteUrl(og[1]));
          } catch(e){}
        }
        const fallbackUrls = Array.from(OUT).slice(0, count);
        if (fallbackUrls.length) {
          // save small cache
          mergeIntoCache(tag, fallbackUrls.map(u=>({ id: normalizeUrlStripQuery(u), url: u })));
          for (const u of fallbackUrls) urls.push(u);
        }
      } catch (e) { dbg('html fallback error', e && e.message ? e.message : e); }
    }

    if (!urls.length) return res.status(200).json({ tag, pagesFetched: 0, totalVideosFound: 0, videos: [], source: 'none', debug: (DEEP_DEBUG ? ['no results from public endpoints'] : undefined) });

    // update recent set
    const newRecent = recentArr.slice();
    for (const p of picked) { const id = p.id || normalizeUrlStripQuery(p.url); newRecent.unshift(id); }
    const deduped = []; const seen = new Set();
    for (const id of newRecent) { if (!seen.has(id)) { seen.add(id); deduped.push(id); } if (deduped.length >= RECENT_MAX) break; }
    saveRecentSet(tag, deduped);

    const totalCached = loadTagCache(tag).items.length;
    res.json({ tag, fetched_at: new Date().toISOString(), pagesFetched: 0, totalVideosFound: totalCached, videos: urls, source: 'public_no_cookie', debug: (DEEP_DEBUG ? ['public_no_cookie mode'] : undefined) });
  } catch (err) {
    console.error('API error', err && err.message ? err.message : err);
    res.status(500).json({ error: err && err.message ? err.message : String(err) });
  }
});

app.listen(PORT, () => { console.log(`topserp-no-cookie-server listening on http://localhost:${PORT}/ (DEEP_DEBUG=${DEEP_DEBUG})`); });