#!/usr/bin/env node
/**
 * build-providers.js — bakes "where is it streaming" into the site, so visitors
 * without a TMDB key see it too. Runs weekly in the site repo's GitHub Action
 * (.github/workflows/providers.yml), which holds the key as a repository secret.
 *
 * Usage:
 *   TMDB_TOKEN=read_access_token node build-providers.js [--dir .] [--regions US,GB]
 *                                                        [--rps 20] [--limit N]
 *   TMDB_KEY=v3_key works too, but the v4 read access token is preferred: it goes
 *   in an Authorization header, so it never appears in a URL.
 *
 * What it does:
 *   1. Reads every TMDB id out of film-db-core.js and film-db-tail.js in --dir.
 *   2. Asks TMDB's /movie/{id}/watch/providers for each (data from JustWatch).
 *      One answer covers every country, so extra regions cost no extra requests.
 *   3. Writes providers-<REGION>.js per region into --dir. The page loads only the
 *      file for the visitor's region.
 *
 * Nothing it writes or prints contains the key: errors report the film id and
 * the HTTP status only, never a URL or a response header.
 *
 * Output format (one line of data per region, films sorted by TMDB id):
 *   window.CMR_PROVIDERS = {region, built, db, names:{providerId:name}, data:"…"}
 *   data is space-separated entries "<idDelta>[:<subs>/<free>/<rentBuy>]", every
 *   number in base 36, each list comma-separated in TMDB's display order. idDelta
 *   is the gap from the previous entry's id. A bare delta means the film is on
 *   nothing in that country; a film missing entirely was not checked (the fetch
 *   failed), so the page treats it as unknown rather than "not streaming".
 */

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const args = process.argv.slice(2);
function getArg(name, def) {
  const i = args.indexOf(name);
  return i === -1 || !args[i + 1] || args[i + 1].startsWith('--') ? def : args[i + 1];
}
const DIR = path.resolve(getArg('--dir', '.'));
const REGIONS = String(getArg('--regions', process.env.REGIONS || 'US')).toUpperCase().split(',').map(s => s.trim()).filter(s => /^[A-Z]{2}$/.test(s));
const RPS = Math.max(1, parseInt(getArg('--rps', '20'), 10) || 20);
const LIMIT = parseInt(getArg('--limit', '0'), 10) || 0;
const CONCURRENCY = 8, RETRIES = 4;
const RB_MAX = 3;   // the page shows three rent/buy storefronts at most
const API = process.env.TMDB_API_BASE || 'https://api.themoviedb.org/3';   // swappable for a fake TMDB in tests

const TOKEN = process.env.TMDB_TOKEN || '', KEY = process.env.TMDB_KEY || '';
if (!TOKEN && !KEY) {
  console.error('Error: set TMDB_TOKEN (the API read access token) or TMDB_KEY in the environment.');
  process.exit(1);
}
if (!REGIONS.length) { console.error('Error: no valid regions (two-letter codes, e.g. US,GB).'); process.exit(1); }

const sleep = ms => new Promise(r => setTimeout(r, ms));

function makeLimiter(rps) {
  let tokens = rps, last = Date.now(), paused = 0;
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

// ─── Read the ids out of the database ────────────────────────────────────────
function loadDB() {
  const sandbox = { window: {} };
  vm.createContext(sandbox);
  for (const f of ['film-db-core.js', 'film-db-tail.js']) {
    const p = path.join(DIR, f);
    if (fs.existsSync(p)) vm.runInContext(fs.readFileSync(p, 'utf8'), sandbox, { filename: f });
  }
  const w = sandbox.window, ids = new Set();
  for (const db of [w.FILM_DB, w.FILM_DB_TAIL]) {
    for (const k in db || {}) {
      const id = parseInt(String(db[k]).split('|')[6], 10);
      if (id > 0) ids.add(id);
    }
  }
  return { ids: [...ids].sort((a, b) => a - b), built: (w.FILM_DB_META || {}).built || '' };
}

// ─── Fetch ───────────────────────────────────────────────────────────────────
async function fetchProviders(id, limiter) {
  const url = `${API}/movie/${id}/watch/providers` + (TOKEN ? '' : `?api_key=${encodeURIComponent(KEY)}`);
  const headers = TOKEN ? { Authorization: 'Bearer ' + TOKEN, Accept: 'application/json' } : { Accept: 'application/json' };
  for (let attempt = 0; ; attempt++) {
    await limiter.take();
    let res;
    try { res = await fetch(url, { headers, signal: AbortSignal.timeout(20000) }); }
    catch (e) {
      if (attempt >= RETRIES) throw new Error('network error');
      await sleep(1000 * 2 ** attempt); continue;
    }
    if (res.ok) return (await res.json()).results || {};
    if (res.status === 404) return {};   // gone from TMDB: on nothing
    if (res.status === 401) throw Object.assign(new Error('HTTP 401'), { auth: true });
    if ((res.status === 429 || res.status >= 500) && attempt < RETRIES) {
      const wait = res.status === 429 ? (parseFloat(res.headers.get('retry-after')) || 2) * 1000 : 1000 * 2 ** attempt;
      if (res.status === 429) limiter.pause(wait);
      await sleep(wait); continue;
    }
    throw new Error('HTTP ' + res.status);
  }
}

// Mirrors fetchProviders in Pages/index.html: subscription, free (incl. with ads),
// and rent/buy merged without duplicates, each in TMDB's display order.
function pickOffers(r, names) {
  const pick = list => (list || []).slice().sort((a, b) => (a.display_priority || 0) - (b.display_priority || 0))
    .filter(x => Number.isInteger(x.provider_id) && x.provider_id > 0 && x.provider_name)
    .map(x => { names[x.provider_id] = String(x.provider_name).slice(0, 40); return x.provider_id; });
  const uniq = a => a.filter((x, i) => a.indexOf(x) === i);
  return { s: uniq(pick(r.flatrate)), f: uniq([...pick(r.free), ...pick(r.ads)]), rb: uniq([...pick(r.rent), ...pick(r.buy)]).slice(0, RB_MAX) };
}

function encode(entries) {
  let prev = 0;
  return entries.map(([id, o]) => {
    const d = (id - prev).toString(36); prev = id;
    if (!o.s.length && !o.f.length && !o.rb.length) return d;
    const l = a => a.map(n => n.toString(36)).join(',');
    return d + ':' + l(o.s) + '/' + l(o.f) + '/' + l(o.rb);
  }).join(' ');
}

async function main() {
  const db = loadDB();
  const ids = LIMIT ? db.ids.slice(0, LIMIT) : db.ids;
  if (!ids.length) { console.error(`Error: no TMDB ids found in ${DIR}/film-db-core.js`); process.exit(1); }
  console.log(`${ids.length} films, regions ${REGIONS.join(',')}, ${RPS} req/s (~${Math.ceil(ids.length / RPS / 60)} min)`);

  const limiter = makeLimiter(RPS);
  const perRegion = Object.fromEntries(REGIONS.map(r => [r, { entries: [], names: {} }]));
  let next = 0, done = 0, failed = 0, authFailed = false;
  const started = Date.now();

  async function worker() {
    while (next < ids.length && !authFailed) {
      const id = ids[next++];
      try {
        const results = await fetchProviders(id, limiter);
        for (const r of REGIONS) perRegion[r].entries.push([id, pickOffers(results[r] || {}, perRegion[r].names)]);
      } catch (e) {
        if (e.auth) { authFailed = true; break; }
        failed++;
        if (failed <= 20) console.log(`  film ${id}: ${e.message}`);
      }
      if (++done % 2000 === 0) console.log(`  ${done}/${ids.length} (${Math.round((Date.now() - started) / 60000)} min, ${failed} failed)`);
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));

  if (authFailed) { console.error('Error: TMDB rejected the credentials (401). Check the TMDB_TOKEN secret.'); process.exit(1); }
  // A run where TMDB was mostly down would otherwise replace good files with thin ones
  if (failed > ids.length * 0.05) { console.error(`Error: ${failed} of ${ids.length} lookups failed; leaving the existing files alone.`); process.exit(1); }

  const built = new Date().toISOString().slice(0, 10);
  for (const r of REGIONS) {
    const { entries, names } = perRegion[r];
    entries.sort((a, b) => a[0] - b[0]);
    const onSomething = entries.filter(e => e[1].s.length || e[1].f.length).length;
    const payload = { region: r, built, db: db.built, names, data: encode(entries) };
    const out = `// Generated by build-providers.js — streaming availability in ${r} for ${entries.length} films (${onSomething} streaming)\n` +
      `// Data from JustWatch, via TMDB. Format described at the top of build-providers.js.\n` +
      `window.CMR_PROVIDERS = ${JSON.stringify(payload)};\n`;
    const file = path.join(DIR, `providers-${r}.js`);
    fs.writeFileSync(file + '.tmp', out);
    fs.renameSync(file + '.tmp', file);
    console.log(`Wrote providers-${r}.js: ${(out.length / 1024).toFixed(0)} KB, ${onSomething} of ${entries.length} films streaming`);
  }
  console.log(`Done in ${Math.round((Date.now() - started) / 60000)} min, ${failed} failed.`);
}

main().catch(e => { console.error('Error: ' + e.message); process.exit(1); });
