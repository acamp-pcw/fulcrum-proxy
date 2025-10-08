// server.js (CommonJS)
const express = require('express');

const app = express();
app.use(express.json({ limit: '2mb' }));

const ALLOWED_PREFIX = '/api/';
const SHARED_SECRET = process.env.SHARED_SECRET || 'replace-me';

// Use Node 18+ built-ins
const { fetch, Headers } = globalThis;

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

    const fwdHeaders = new Headers({
      Authorization: `Bearer ${process.env.FULCRUM_TOKEN}`,
      'Content-Type': headers['content-type'] || 'application/json',
    });

    const resp = await fetch(url, {
      method: (method || 'GET').toUpperCase(),
      headers: fwdHeaders,
      body: body ? JSON.stringify(body) : undefined,
    });

    res.status(resp.status);
    res.set('content-type', resp.headers.get(


