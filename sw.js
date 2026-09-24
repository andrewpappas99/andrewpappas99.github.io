// Service worker for the film database only.
//
// film-db-core.js (~3 MB over the wire) and film-db-tail.js (~4.5 MB) are the
// database. GitHub Pages caches them for ten minutes, so most visits revalidate
// the core before the page can start. This serves the copies already on the
// device at once and refreshes them in the background: a rebuilt database shows
// up on the visit after it lands. Everything else is left to the browser, so the
// page itself is never stale.
const CACHE = 'cmr-db-v2';   // v2: the split database; activate drops v1's film-db.js
const DB_PATH = /\/film-db(-core|-tail)?\.js$/;

self.addEventListener('install', e => { self.skipWaiting(); });
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  if (e.request.method !== 'GET' || url.origin !== self.location.origin || !DB_PATH.test(url.pathname)) return;
  e.respondWith(caches.open(CACHE).then(async cache => {
    const cached = await cache.match(e.request);
    const refresh = fetch(e.request).then(async res => {
      if (!res || !res.ok) return res;
      // A changed database is only used on the next load, so tell the page,
      // which offers a reload rather than quietly showing yesterday's data.
      const changed = !cached || (cached.headers.get('etag') || cached.headers.get('last-modified') || '') !==
        (res.headers.get('etag') || res.headers.get('last-modified') || 'x');
      await cache.put(e.request, res.clone());
      if (cached && changed) {
        const clients = await self.clients.matchAll({ type: 'window' });
        clients.forEach(c => c.postMessage({ type: 'db-updated' }));
      }
      return res;
    }).catch(() => null);
    if (cached) { e.waitUntil(refresh); return cached; }
    const fresh = await refresh;
    return fresh || new Response('', { status: 503 });
  }));
});
