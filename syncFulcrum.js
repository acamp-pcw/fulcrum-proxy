// syncFulcrum.js
// Full auto-discovery mirror for all Fulcrum /api/.../list endpoints.
// Runs manually or as a Render cron job.

import pg from "pg";
import fetch from "node-fetch";

const { Pool } = pg;
const pool = new Pool({
  connectionString: process.env.DATABASE_URL // ← Render Postgres
});

const PROXY_BASE   = process.env.PROXY_BASE   || "https://<YOUR_PROXY>.onrender.com";
const PROXY_SECRET = process.env.SHARED_SECRET || "<YOUR_PROXY_SECRET>";

/**
 * Fetch JSON data from the Fulcrum proxy.
 * Uses same payload as your working proxy calls.
 */
async function fetchJSON(path) {
  const resp = await fetch(`${PROXY_BASE}/call`, {
    method: "POST",
    headers: {
      "x-proxy-secret": PROXY_SECRET,
      "content-type": "application/json"
    },
    body: JSON.stringify({
      path,
      method: "POST",                    // proxy decides real verb upstream
      body: { DateFrom: "1900-01-01" },  // standard default body
      autoPage: {
        take: 500,
        maxPages: 100,
        sortField: "CreatedUtc",
        sortDir: "Ascending"
      }
    })
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${path} → ${resp.status}: ${text.slice(0, 200)}`);
  }
  return await resp.json();
}

/**
 * Ensure the logging table exists.
 */
async function ensureTables(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS mirror_log (
      resource TEXT,
      rowcount INTEGER,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
}

/**
 * Fetch a resource and store it as JSONB.
 */
async function syncResource(client, path) {
  const resource = path.split("/")[2]; // e.g. jobs, items, inventory
  console.log(`→ syncing ${resource}`);
  const data = await fetchJSON(path);
  if (!Array.isArray(data)) {
    console.error(`⚠ ${resource}: response was not an array`);
    return;
  }

  await client.query(`CREATE TABLE IF NOT EXISTS ${resource} (payload JSONB)`);
  await client.query(`TRUNCATE ${resource}`);
  const insert = `INSERT INTO ${resource}(payload) VALUES ($1)`;
  for (const row of data) {
    await client.query(insert, [row]);
  }
  await client.query(
    `INSERT INTO mirror_log(resource,rowcount,synced_at)
     VALUES ($1,$2,NOW())`,
    [resource, data.length]
  );
  console.log(`✓ ${resource}: ${data.length} rows`);
}

/**
 * Main entry point.
 * - Loads the schema from proxy
 * - Extracts all /list endpoints
 * - Mirrors each endpoint into Postgres
 */
async function main() {
  const client = await pool.connect();
  try {
    await ensureTables(client);

    console.log("Loading schema from proxy...");
    const schemaResp = await fetch(`${PROXY_BASE}/schema`, {
      headers: { "x-proxy-secret": PROXY_SECRET }
    });

    if (!schemaResp.ok) {
      throw new Error(`Schema fetch failed: ${schemaResp.status}`);
    }

    const schema = await schemaResp.json();
    const allPaths = (schema.resources || [])
      .map(r => r.op.path)
      .filter(p => p.startsWith("/api/") && /\/list($|\/)/.test(p));

    console.log(`Discovered ${allPaths.length} list endpoints.`);

    for (const path of allPaths) {
      try {
        await syncResource(client, path);
      } catch (e) {
        console.error(`✗ failed ${path}:`, e.message);
      }
    }

    console.log("Mirror sync complete", new Date().toISOString());
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(e => {
  console.error("Mirror job failed:", e);
  process.exit(1);
});
