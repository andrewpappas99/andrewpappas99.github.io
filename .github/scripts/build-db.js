#!/usr/bin/env node
/**
 * build-db.js — builds the film database the app ships with, in two files:
 *   Pages/film-db-core.js  the CORE_FILMS most popular films, loaded with the page
 *   Pages/film-db-tail.js  everything else, loaded in the background afterwards
 * Nearly every film anyone logs is in the core, so the page no longer waits on
 * the whole database; the tail feeds recommendations, filmographies and blind spots.
 *
 * Usage:
 *   TMDB_KEY=your_key node build-db.js [--count 30000] [--full] [--rps 20]
 *                                      [--concurrency 8] [--max-age-days 90]
 *                                      [--core 20000] [--dry-run]
 *                                      [--out Pages] [--cache build-cache.json]
 *
 *   The key comes from the TMDB_KEY environment variable so it stays out of the
 *   shell history and the process list. (--key still works, with a warning.)
 *   TMDB_TOKEN, the v4 read access token, works instead: it is sent in a header,
 *   so it never appears in a URL. The site repo's weekly GitHub Action uses it,
 *   with --out . (the site root) and a cache kept on the repo's db-cache branch.
 *
 * What it does:
 *   1. Downloads TMDB's free daily movie-id export and takes the top --count
 *      films by popularity. That ordering is free: no per-film request needed.
 *   2. Reads build-cache.json, the per-film record cache from earlier runs, and
 *      seeds it from the film-db.js on disk (once the DB carries TMDB ids).
 *   3. Fetches only the films the cache lacks or holds stale: never fetched,
 *      older than --max-age-days, released in the last two years and older than
 *      a week, or listed by TMDB's /movie/changes since the last build.
 *   4. Paces requests with a token bucket (--rps, default 20/s) rather than the
 *      old 40-per-10.5-seconds batches — TMDB dropped that limit in 2019 and now
 *      allows around 40/s. A 30,000-film build takes ~25 minutes instead of ~3
 *      hours; an incremental run fetches a few hundred films and takes minutes.
 *   5. Retries 429 (honouring Retry-After) and 5xx/network errors with backoff,
 *      then a second pass, and records anything still failing in
 *      build-failures.json with a reason. The cache is checkpointed as it goes,
 *      so an interrupted run resumes where it stopped.
 *   6. Assembles the database in popularity order from the cache. A film whose
 *      fetch failed this run keeps its cached record, so a bad hour never
 *      produces a smaller database than the one on disk.
 *
 * Output format per film:
 *   "Title||Year": "Director|Writers|Actors|Genres|VoteAvg|RuntimeMins|TmdbId|Poster"
 *   Poster is TMDB's poster path (the app prefixes image.tmdb.org). Cached
 *   records that predate the column are fetched again once.
 *   VoteAvg is blank for films with fewer than MIN_VOTES TMDB votes, whose
 *   averages are noise. Cached records from before that rule are fetched again once.
 *   Two different films sharing a Title||Year are both kept: the more popular one
 *   under the plain key, the other under "Title||Year#2", so a director's
 *   filmography and the recommender still see it.
 */

const https = require('https');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

// ─── Parse args ──────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
function getArg(name, def) {
  const i = args.indexOf(name);
  if (i === -1) return def;
  const v = args[i + 1];
  if (!v || v.startsWith('--')) {
    console.error(`Error: ${name} requires a value.`);
    process.exit(1);
  }
  return v;
}
function intArg(name, def) {
  const raw = getArg(name, String(def));
  const n = parseInt(raw, 10);
  if (!Number.isInteger(n) || n < 1) { console.error(`Error: ${name} must be a positive integer (got "${raw}").`); process.exit(1); }
  return n;
}

const ROOT = __dirname;
const OUT_DIR = path.resolve(getArg('--out', path.join(ROOT, 'Pages')));
const CORE_PATH = path.join(OUT_DIR, 'film-db-core.js');
const TAIL_PATH = path.join(OUT_DIR, 'film-db-tail.js');
const LEGACY_PATH = path.join(OUT_DIR, 'film-db.js');   // the single-file format, before the split
const CACHE_PATH = path.resolve(getArg('--cache', path.join(ROOT, 'build-cache.json')));
const FAILURES_PATH = path.join(path.dirname(CACHE_PATH), 'build-failures.json');

const WRITING_JOBS = new Set(['Screenplay', 'Writer', 'Story', 'Original Story', 'Script', 'Adaptation']);
const REQUEST_TIMEOUT_MS = 15000;
const MAX_ATTEMPTS = 6;                 // per request, with backoff
const CHECKPOINT_EVERY = 500;           // records fetched between cache writes (Ctrl+C saves too)
const FRESH_YEARS = 2;                  // films this recent are re-fetched weekly
const FRESH_MAX_AGE_DAYS = 7;
const MISS_RECHECK_DAYS = { 'no-date': 7, 'not-found': 30, 'no-title': 30 };
const CHANGES_MAX_DAYS = 60;            // beyond this, rely on max-age instead
const CORE_FILMS = 20000;               // default --core: covers ~99% of the films in real libraries
const MIN_COVERAGE = 0.95;              // warn when fewer target ids have a record
// Below this many TMDB votes the average is noise — three ten-star votes make a
// 5.0 — and the app has no vote counts to discount it with. Past the top ~30k
// films most of the tail is in that range, so those averages are left blank and
// the app treats the film as unrated by the crowd rather than acclaimed.
const MIN_VOTES = 10;

// ─── HTTP ────────────────────────────────────────────────────────────────────
const agent = new https.Agent({ keepAlive: true, maxSockets: 16 });
// Set in main from TMDB_TOKEN; sent only to the API host, never to files.tmdb.org
const auth = { token: '' };

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    const headers = auth.token && url.startsWith('https://api.themoviedb.org/') ? { Authorization: 'Bearer ' + auth.token } : {};
    const req = https.get(url, { agent, headers }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    // A hung socket used to hold a whole batch forever, with no output to say so.
    req.setTimeout(REQUEST_TIMEOUT_MS, () => req.destroy(new Error('timeout')));
    req.on('error', reject);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Steady pacing: one token per 1/rps seconds, up to a small burst, so requests
// spread evenly instead of 40 at once followed by a ten-second silence.
function makeLimiter(rps) {
  let tokens = rps, last = Date.now();
  let paused = 0;
  return {
    async take() {
      for (;;) {
        const now = Date.now();
        if (paused > now) { await sleep(paused - now); continue; }
        tokens = Math.min(rps, tokens + (now - last) / 1000 * rps);
        last = now;
        if (tokens >= 1) { tokens -= 1; return; }
        await sleep(Math.ceil((1 - tokens) / rps * 1000));
      }
    },
    pause(ms) { paused = Math.max(paused, Date.now() + ms); }
  };
}

// Everything the build knows how to do over the network, kept swappable so the
// pipeline can be tested against a fake TMDB.
const deps = {
  fetchUrl,
  sleep,
  now: () => Date.now()
};

// One JSON request with the retry policy: 404 is a miss (null); 429 pauses the
// limiter for Retry-After (default 2 s) and retries; 5xx, timeouts and socket
// errors back off exponentially; anything else after MAX_ATTEMPTS throws.
async function fetchJSON(url, limiter) {
  let wait = 1000, lastErr;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    if (limiter) await limiter.take();
    let r;
    try { r = await deps.fetchUrl(url); }
    catch (e) { lastErr = e; await deps.sleep(wait); wait = Math.min(wait * 2, 30000); continue; }
    if (r.status === 200) {
      try { return JSON.parse(r.body.toString('utf8')); }
      catch (e) { lastErr = new Error('bad JSON'); await deps.sleep(wait); wait = Math.min(wait * 2, 30000); continue; }
    }
    if (r.status === 404) return null;
    if (r.status === 401) throw Object.assign(new Error('TMDB rejected the API key (401)'), { fatal: true });
    if (r.status === 429) {
      const ra = parseFloat(r.headers && r.headers['retry-after']);
      const ms = Number.isFinite(ra) && ra > 0 ? ra * 1000 : 2000;
      if (limiter) limiter.pause(ms); else await deps.sleep(ms);
      lastErr = new Error('rate limited'); continue;
    }
    if (r.status >= 500) { lastErr = new Error('HTTP ' + r.status); await deps.sleep(wait); wait = Math.min(wait * 2, 30000); continue; }
    throw new Error('HTTP ' + r.status);
  }
  throw lastErr || new Error('gave up');
}

// ─── Step 1: the daily export ────────────────────────────────────────────────
function exportUrlFor(date) {
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  return `https://files.tmdb.org/p/exports/movie_ids_${mm}_${dd}_${date.getUTCFullYear()}.json.gz`;
}

// Lines of {id, popularity, adult, video}; keeps released feature films, sorted
// by popularity, top `count`.
function parseExport(text, count) {
  const ids = [];
  for (const line of text.split('\n')) {
    if (!line.trim()) continue;
    try {
      const o = JSON.parse(line);
      if (!o.adult && !o.video && o.id && o.popularity > 0) ids.push({ id: o.id, popularity: o.popularity });
    } catch { /* skip malformed lines */ }
  }
  ids.sort((a, b) => b.popularity - a.popularity);
  return { total: ids.length, ids: ids.slice(0, count).map(m => m.id) };
}

async function downloadMovieIds(count) {
  // TMDB publishes the export a few hours into the day; yesterday's is always there.
  for (let back = 1; back <= 3; back++) {
    const d = new Date(deps.now() - back * 86400000);
    const url = exportUrlFor(d);
    process.stdout.write(`Downloading TMDB export for ${url.slice(-18, -8)}...`);
    // A dropped connection used to abort the build instead of trying the day before.
    let r;
    try { r = await deps.fetchUrl(url); }
    catch (e) { process.stdout.write(` ${e.message}\n`); continue; }
    if (r.status !== 200) { process.stdout.write(` HTTP ${r.status}\n`); continue; }
    const text = zlib.gunzipSync(r.body).toString('utf8');
    process.stdout.write(' done\n');
    const { total, ids } = parseExport(text, count);
    console.log(`Found ${total} released films in the export, using the top ${ids.length} by popularity`);
    return ids;
  }
  throw new Error('Could not download the TMDB id export (tried the last three days).');
}

// ─── Step 2: one film → one record ───────────────────────────────────────────
// Returns {rec:{title,year,value}} for a usable film, or {miss:'no-title'|'no-date'}
// for one TMDB returned but the DB cannot hold. A 404 arrives as data===null.
function mapMovie(data) {
  if (!data) return { miss: 'not-found' };
  if (!data.title) return { miss: 'no-title' };
  const year = parseInt((data.release_date || '').slice(0, 4), 10);
  if (!year) return { miss: 'no-date' };            // announced, unreleased, undated
  const crew = (data.credits && data.credits.crew) || [];
  const cast = (data.credits && data.credits.cast) || [];
  const director = (crew.find(c => c.job === 'Director') || {}).name || '';
  const writers = [...new Set(crew.filter(c => WRITING_JOBS.has(c.job)).map(c => c.name))].slice(0, 3);
  const actors = cast.slice(0, 10).map(c => c.name);
  const genres = (data.genres || []).map(g => g.name);
  const votes = parseInt(data.vote_count, 10) || 0;
  const voteAvg = data.vote_average && votes >= MIN_VOTES ? (data.vote_average / 2).toFixed(1) : '';
  const runtime = parseInt(data.runtime, 10) || 0;
  // Pipes and commas are the record separators, so they cannot appear in a field.
  const clean = s => String(s || '').replace(/[|,]/g, ' ').trim();
  const poster = POSTER_RE.test(String(data.poster_path || '')) ? data.poster_path : '';
  return { rec: {
    title: data.title,
    year,
    poster,
    votes,
    value: `${clean(director)}|${writers.map(clean).join(',')}|${actors.map(clean).join(',')}|${genres.map(clean).join(',')}|${voteAvg}|${runtime}|${data.id}|${poster}`
  } };
}
const POSTER_RE = /^\/[A-Za-z0-9_-]{6,80}\.(jpg|png|webp)$/;

// With a bearer token there is no key, and nothing to put in the URL
const keyParam = key => key ? `&api_key=${encodeURIComponent(key)}` : '';
function movieUrl(id, key) {
  return `https://api.themoviedb.org/3/movie/${id}?append_to_response=credits&language=en-US${keyParam(key)}`;
}

// ─── The cache ───────────────────────────────────────────────────────────────
// { meta:{lastBuilt}, films:{ [id]: {at, title, year, poster, votes, value} | {at, miss} } }
function loadCache(p) {
  try {
    const c = JSON.parse(fs.readFileSync(p, 'utf8'));
    if (c && typeof c === 'object' && c.films && typeof c.films === 'object') return { meta: c.meta || {}, films: c.films };
  } catch { /* no cache yet */ }
  return { meta: {}, films: {} };
}
function saveCache(p, cache) {
  const tmp = p + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(cache), 'utf8');
  fs.renameSync(tmp, p);
}

// The database on disk, read back into {key: value} plus its build date. Takes
// the core and tail files concatenated (or a legacy single film-db.js); the tail
// is merged after the core, the same way the app does it.
function parseExistingDB(js) {
  if (!js) return { db: {}, built: null };
  const sandbox = { window: {} };
  try { vm.runInContext(js, vm.createContext(sandbox), { timeout: 20000 }); }
  catch { return { db: {}, built: null }; }
  const meta = sandbox.window.FILM_DB_META || {};
  const db = sandbox.window.FILM_DB || {};
  Object.assign(db, sandbox.window.FILM_DB_TAIL || {});
  return { db, built: typeof meta.built === 'string' ? meta.built : null };
}
function readExistingDB() {
  if (fs.existsSync(CORE_PATH)) return fs.readFileSync(CORE_PATH, 'utf8') + '\n' + (fs.existsSync(TAIL_PATH) ? fs.readFileSync(TAIL_PATH, 'utf8') : '');
  return fs.existsSync(LEGACY_PATH) ? fs.readFileSync(LEGACY_PATH, 'utf8') : '';
}

// Seed the cache from an existing database, so the first run under this script
// still reuses everything the old one fetched — provided the rows carry ids.
function seedCacheFromDB(cache, db, built) {
  const at = built ? Date.parse(built + 'T00:00:00Z') : 0;
  let seeded = 0;
  for (const key in db) {
    const value = db[key];
    const parts = value.split('|');
    const id = parseInt(parts[6], 10);
    if (!Number.isFinite(id) || cache.films[id]) continue;
    const i = key.lastIndexOf('||');
    const title = key.slice(0, i), year = parseInt(key.slice(i + 2), 10);
    if (!Number.isFinite(year)) continue;
    const entry = { at, title, year, value };
    if (parts.length >= 8) entry.poster = parts[7];   // older rows have no poster column and are refetched once
    cache.films[id] = entry;
    seeded++;
  }
  return seeded;
}

// Is a cached entry due for a fresh fetch?
function isStale(entry, now, opts) {
  if (!entry) return true;
  const ageDays = (now - (entry.at || 0)) / 86400000;
  if (entry.miss) return ageDays > (MISS_RECHECK_DAYS[entry.miss] || 30);
  if (entry.poster === undefined) return true;   // record predates the poster column
  if (entry.votes === undefined) return true;    // predates the vote-count floor; refetched once
  if (ageDays > opts.maxAgeDays) return true;
  const thisYear = new Date(now).getUTCFullYear();
  if (entry.year >= thisYear - FRESH_YEARS && ageDays > FRESH_MAX_AGE_DAYS) return true;
  return false;
}

// ─── Step 3: what changed since the last build ───────────────────────────────
// /movie/changes lists ids edited in a window of up to 14 days, 100 per page.
function changeWindows(fromMs, toMs) {
  const out = [];
  const day = 86400000;
  for (let s = fromMs; s < toMs; s += 14 * day) out.push([s, Math.min(s + 14 * day, toMs)]);
  return out;
}
function isoDay(ms) { return new Date(ms).toISOString().slice(0, 10); }

async function changedSince(fromMs, key, limiter) {
  const now = deps.now();
  if (!fromMs || now - fromMs > CHANGES_MAX_DAYS * 86400000) return null;   // too long ago to be worth it
  const ids = new Set();
  for (const [s, e] of changeWindows(fromMs, now)) {
    for (let page = 1; page <= 500; page++) {
      const url = `https://api.themoviedb.org/3/movie/changes?start_date=${isoDay(s)}&end_date=${isoDay(e)}&page=${page}${keyParam(key)}`;
      const data = await fetchJSON(url, limiter);
      if (!data || !Array.isArray(data.results)) break;
      for (const r of data.results) if (r && r.id) ids.add(r.id);
      if (page >= (data.total_pages || 1)) break;
    }
  }
  return ids;
}

// ─── Step 4: fetch what is needed ────────────────────────────────────────────
function makeStats() {
  return { fetched: 0, reused: 0, 'no-date': 0, 'not-found': 0, 'no-title': 0, errored: 0, retriedOk: 0, collided: 0 };
}

async function fetchFilms(ids, key, cache, opts, stats, onProgress) {
  const limiter = makeLimiter(opts.rps);
  const failures = new Map();   // id -> reason
  let inFlight = 0, next = 0, sinceCheckpoint = 0, done = 0;
  const startedAt = deps.now();

  const one = async id => {
    let result;
    try { result = mapMovie(await fetchJSON(movieUrl(id, key), limiter)); }
    catch (e) {
      if (e.fatal) throw e;
      failures.set(id, e.message || 'error'); stats.errored++;
      return;
    }
    if (result.rec) { cache.films[id] = Object.assign({ at: deps.now() }, result.rec); stats.fetched++; }
    else { cache.films[id] = { at: deps.now(), miss: result.miss }; stats[result.miss]++; }
  };

  await new Promise((resolve, reject) => {
    let failed = false;
    const pump = () => {
      if (failed) return;
      if (next >= ids.length && inFlight === 0) return resolve();
      while (inFlight < opts.concurrency && next < ids.length) {
        const id = ids[next++];
        inFlight++;
        one(id).then(() => {
          inFlight--; done++; sinceCheckpoint++;
          if (sinceCheckpoint >= CHECKPOINT_EVERY) { sinceCheckpoint = 0; if (opts.cachePath) saveCache(opts.cachePath, cache); }
          if (onProgress) onProgress(done, ids.length, startedAt, stats, failures.size);
          pump();
        }, e => { failed = true; reject(e); });
      }
    };
    pump();
  });
  return failures;
}

// ─── Step 5: assemble ────────────────────────────────────────────────────────
// Popularity order in, film-db.js order out. A film with no usable record (a miss,
// or never fetched) is skipped; two films with one Title||Year both survive.
function assemble(ids, cache, stats) {
  const db = {};
  const missing = [];
  const collisions = [];
  for (const id of ids) {
    const e = cache.films[id];
    if (!e || !e.value) { if (!e || !e.miss) missing.push(id); continue; }
    let key = `${e.title}||${e.year}`;
    if (key in db) {
      let n = 2; while ((`${key}#${n}`) in db) n++;
      collisions.push({ id, key, storedAs: `${key}#${n}` });
      key = `${key}#${n}`;
      if (stats) stats.collided++;
    }
    db[key] = e.value;
  }
  return { db, missing, collisions };
}

// ─── Step 6: write ───────────────────────────────────────────────────────────
// The first coreCount films (popularity order) go in the core, the rest in the
// tail. Title||Year#2 keys are numbered across the whole database before the
// split, so a tail film colliding with a core one still gets a unique key.
function renderFilmDB(db, built, coreCount = CORE_FILMS) {
  const all = Object.entries(db);
  const count = all.length, core = all.slice(0, coreCount), tail = all.slice(coreCount);
  const body = rows => rows.map(([k, v]) => `  ${JSON.stringify(k)}:${JSON.stringify(v)}`).join(',\n');
  const format = `// Format: "Title||Year": "Director|Writers|Actors|Genres|VoteAvg|RuntimeMins|TmdbId|Poster"\n// A second film sharing a Title||Year is stored under "Title||Year#2".\n`;
  return {
    core: `// Generated by build-db.js — the ${core.length} most popular of ${count} films; the rest are in film-db-tail.js\n// Built: ${built}\n${format}` +
      `window.FILM_DB_META = {built:${JSON.stringify(built)}, count:${count}, core:${core.length}, tail:${tail.length}, ids:true, posters:true};\nwindow.FILM_DB = {\n${body(core)}\n};\n`,
    tail: `// Generated by build-db.js — films ${core.length + 1} to ${count} by popularity; film-db-core.js holds the rest\n// Built: ${built}\n${format}` +
      `// The app loads this after the page is up and appends it to FILM_DB in this order.\nwindow.FILM_DB_TAIL = {\n${body(tail)}\n};\n`
  };
}

function writeAtomic(p, text) {
  const tmp = p + '.tmp';
  fs.writeFileSync(tmp, text, 'utf8');
  fs.renameSync(tmp, p);                // never leaves a truncated file behind
}
function writeFilmDB(db, built, coreCount, paths = { core: CORE_PATH, tail: TAIL_PATH, legacy: LEGACY_PATH }) {
  const { core, tail } = renderFilmDB(db, built, coreCount);
  // Tail first: a page that loads mid-write then gets an old core with a new
  // tail, which merges harmlessly, rather than a new core pointing at nothing.
  writeAtomic(paths.tail, tail);
  writeAtomic(paths.core, core);
  // The single-file database is superseded; left behind it would be deployed stale.
  if (paths.legacy && fs.existsSync(paths.legacy)) { fs.unlinkSync(paths.legacy); console.log('Removed the old single-file Pages/film-db.js'); }
  return { core: Buffer.byteLength(core), tail: Buffer.byteLength(tail) };
}

// ─── Main ─────────────────────────────────────────────────────────────────────
function fmtSecs(s) { s = Math.round(s); return s >= 60 ? `${Math.floor(s / 60)}m ${s % 60}s` : `${s}s`; }

async function main() {
  const legacyKey = getArg('--key', '');
  if (legacyKey) console.error('Note: --key puts the API key in your shell history and the process list. Prefer TMDB_KEY=… in the environment.');
  // A short v3 key pasted into TMDB_TOKEN (32 hex characters) still goes in the URL
  const token = (process.env.TMDB_TOKEN || '').trim();
  const API_KEY = (process.env.TMDB_KEY || legacyKey || (/^[0-9a-f]{32}$/i.test(token) ? token : '')).trim();
  if (!API_KEY && token) auth.token = token;
  if (!API_KEY && !auth.token) {
    console.error('Error: set TMDB_KEY (or TMDB_TOKEN, the read access token) in the environment. Get a free key at https://www.themoviedb.org/settings/api');
    process.exit(1);
  }
  const opts = {
    count: intArg('--count', 30000),
    rps: intArg('--rps', 20),
    concurrency: intArg('--concurrency', 8),
    maxAgeDays: intArg('--max-age-days', 90),
    core: intArg('--core', CORE_FILMS),
    full: args.includes('--full'),
    dryRun: args.includes('--dry-run'),
    cachePath: CACHE_PATH
  };
  const started = deps.now();
  console.log(`Building FILM_DB: top ${opts.count} films by TMDB popularity at ~${opts.rps} req/s`);

  // Cache, seeded from whatever database is already on disk
  const cache = loadCache(CACHE_PATH);
  const existing = parseExistingDB(readExistingDB());
  const seeded = seedCacheFromDB(cache, existing.db, existing.built);
  const cachedBefore = Object.keys(cache.films).length;
  console.log(`Cache: ${cachedBefore} films${seeded ? ` (${seeded} seeded from the ${existing.built || 'existing'} database)` : ''}${!seeded && Object.keys(existing.db).length && !cachedBefore ? ' — the database on disk has no TMDB ids, so this first run fetches everything' : ''}`);

  // Ctrl+C mid-build keeps everything fetched so far; the next run resumes there.
  process.once('SIGINT', () => {
    console.log('\nInterrupted — saving the cache so the next run picks up where this one stopped.');
    try { saveCache(CACHE_PATH, cache); } catch (e) { console.error('Could not save the cache:', e.message); }
    process.exit(130);
  });

  const ids = await downloadMovieIds(opts.count);

  // Decide what to fetch
  const now = deps.now();
  let changed = null;
  const lastBuilt = cache.meta.lastBuilt ? Date.parse(cache.meta.lastBuilt) : (existing.built ? Date.parse(existing.built + 'T00:00:00Z') : 0);
  if (!opts.full && lastBuilt) {
    process.stdout.write('Asking TMDB which films changed since the last build...');
    try { changed = await changedSince(lastBuilt, API_KEY, makeLimiter(opts.rps)); process.stdout.write(changed ? ` ${changed.size} ids\n` : ' too long ago, using max-age instead\n'); }
    catch (e) { if (e.fatal) throw e; process.stdout.write(` skipped (${e.message})\n`); }
  }
  const stats = makeStats();
  const todo = ids.filter(id => opts.full || isStale(cache.films[id], now, opts) || (changed && changed.has(id)));
  stats.reused = ids.length - todo.length;
  console.log(`${todo.length} films to fetch, ${stats.reused} reused from the cache`);
  if (opts.dryRun) { console.log('Dry run — nothing fetched or written.'); return; }

  // Fetch, then one more pass over whatever errored
  const progress = (done, total, startedAt, st, failing) => {
    const elapsed = (deps.now() - startedAt) / 1000, rate = done / Math.max(elapsed, 1);
    const eta = rate > 0 ? fmtSecs((total - done) / rate) : '?';
    process.stdout.write(`\r${done}/${total} fetched | ${st['no-date']} undated | ${st['not-found']} gone | ${failing} errored | ${rate.toFixed(1)}/s | ETA ${eta}   `);
  };
  let failures = new Map();
  // A rejected key or anything else fatal still keeps what was fetched before it.
  if (todo.length) try {
    failures = await fetchFilms(todo, API_KEY, cache, opts, stats, progress);
    process.stdout.write('\n');
    if (failures.size) {
      console.log(`Retrying ${failures.size} films that errored…`);
      await deps.sleep(5000);
      const before = stats.errored;
      stats.errored = 0;
      const again = await fetchFilms([...failures.keys()], API_KEY, cache, Object.assign({}, opts, { rps: Math.max(2, Math.floor(opts.rps / 2)), concurrency: 2 }), stats, progress);
      process.stdout.write('\n');
      stats.retriedOk = before - again.size;
      failures = again;
    }
  } catch (e) {
    saveCache(CACHE_PATH, cache);
    throw e;
  }
  cache.meta.lastBuilt = new Date(now).toISOString();
  saveCache(CACHE_PATH, cache);

  // Assemble and write
  const built = new Date(now).toISOString().slice(0, 10);
  const { db, missing, collisions } = assemble(ids, cache, stats);
  const count = Object.keys(db).length;
  const coverage = ids.length ? (ids.length - missing.length) / ids.length : 0;

  const failureReport = [
    ...[...failures.entries()].map(([id, reason]) => ({ id, reason, kept: !!(cache.films[id] && cache.films[id].value) })),
    ...collisions.map(c => ({ id: c.id, reason: 'collision', key: c.key, storedAs: c.storedAs }))
  ];
  fs.writeFileSync(FAILURES_PATH, JSON.stringify({ built, failures: failureReport }, null, 2), 'utf8');

  if (!count) { console.error('\nError: no films to write — refusing to overwrite the database.'); process.exit(1); }
  if (coverage < MIN_COVERAGE) console.error(`\nWarning: only ${(coverage * 100).toFixed(1)}% of the target ids have a record (${missing.length} missing). Writing anyway — every film with a cached record is kept.`);
  const bytes = writeFilmDB(db, built, opts.core);

  const prev = Object.keys(existing.db).length;
  const mb = n => (n / 1024 / 1024).toFixed(1) + ' MB';
  console.log(`\nDone in ${fmtSecs((deps.now() - started) / 1000)}: ${count} films written${prev ? ` (was ${prev})` : ''} — core ${Math.min(count, opts.core)} films ${mb(bytes.core)}, tail ${mb(bytes.tail)}`);
  console.log(`  fetched ${stats.fetched} · reused ${stats.reused} · no release date ${stats['no-date']} · not on TMDB ${stats['not-found']} · no title ${stats['no-title']}`);
  console.log(`  recovered on retry ${stats.retriedOk} · still failing ${failures.size} · title collisions kept ${stats.collided}`);
  if (failures.size) {
    const keptOld = [...failures.keys()].filter(id => cache.films[id] && cache.films[id].value).length;
    console.log(`  ${failures.size} films could not be fetched this run${keptOld ? ` (${keptOld} kept their previous record)` : ''} — see build-failures.json`);
  }
  if (OUT_DIR === path.join(ROOT, 'Pages')) console.log(`\nDeploy Pages/film-db-core.js and Pages/film-db-tail.js alongside index.html and sw.js.`);
}

module.exports = { MIN_VOTES, downloadMovieIds, parseExport, mapMovie, isStale, assemble, seedCacheFromDB, parseExistingDB, renderFilmDB, writeFilmDB, fetchJSON, fetchFilms, changeWindows, makeLimiter, makeStats, deps, exportUrlFor };

if (require.main === module) {
  main().catch(e => { console.error('\nFatal error:', e.message); process.exit(1); });
}
