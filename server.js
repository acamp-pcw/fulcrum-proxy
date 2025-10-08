import express from 'express';
// ❌ remove: import fetch from 'node-fetch';

const app = express();
app.use(express.json({ limit: '2mb' }));

const ALLOWED_PREFIX = '/api/';
const SHARED_SECRET = process.env.SHARED_SECRET || 'replace-me';

app.post('/call', async (req, res) => {
  try {
    const { method, path, query = {}, headers = {}, body, secret } = req.body || {};

    if (!path || !path.startsWith(ALLOWED_PREFIX)) {
      return res.status(400).json({ error: 'Path not allowed' });
    }
    if (secret !== SHARED_SECRET) {
      return res.status(401).json({ error: 'Invalid secret' });
    }

    const qs = Object.keys(query).length ? '?' + new URLSearchParams(query) : '';
    const url = `https://api.fulcrumpro.com${path}${qs}`;

    // ✅ Use the global Headers provided by Node 18+
    const fwdHeaders = new Headers({
      'Authorization': `Bearer ${process.env.FULCRUM_TOKEN}`,
      'Content-Type': headers['content-type'] || 'application/json'
    });

    const resp = await fetch(url, {
      method: (method || 'GET').toUpperCase(),
      headers: fwdHeaders,
      body: body ? JSON.stringify(body) : undefined
    });

    res.status(resp.status);
    res.set('content-type', resp.headers.get('content-type') || 'application/json');
    resp.body.pipe(res);
  } catch (e) {
    console.error('Proxy error:', e); // helpful logs in Render
    res.status(500).json({ error: String(e) });
  }
});

app.get('/healthz', (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 300

