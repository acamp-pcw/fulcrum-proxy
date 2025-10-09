// syncFulcrum.js
// Mirrors all Fulcrum /api/.../list endpoints into a Postgres database.
// Scheduled via Render cron every 15 min.

import pg from "pg";
import fetch from "node-fetch";

const { Pool } = pg;
const pool = new Pool({
  connectionString: process.env.DATABASE_URL // ← Render Postgres
});

const PROXY_BASE = process.env.PROXY_BASE || "https://<YOUR_PROXY>.onrender.com";
const PROXY_SECRET = process.env.SHARED_SECRET || "<YOUR_PROXY_SECRET>";

async function fetchJSON(path) {
  const method = path.endsWith("/list") ? "POST" : "GET";

  const listBody = { DateFrom: "1900-01-01", DateTo: "2100-01-01" };

  const resp = await fetch(`${PROXY_BASE}/call`, {
    method: "POST",
    headers: {
      "x-proxy-secret": PROXY_SECRET,
      "content-type": "application/json"
    },
    body: JSON.stringify({
      path,
      method,
      body: listBody,       // 👈 add this
      autoPage: {
        take: 500,
        maxPages: 100,
        sortField: "UpdatedUtc",
        sortDir: "Ascending"
      }
    })
  });

  if (!resp.ok) throw new Error(`${path} → ${resp.status}`);
  return await resp.json();
}



async function ensureTables(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS mirror_log (
      resource TEXT,
      rowcount INTEGER,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
}

async function syncResource(client, path) {
  const resource = path.split("/")[2]; // e.g., jobs, items, inventory
  console.log(`→ syncing ${resource}`);
  const data = await fetchJSON(path);
  if (!Array.isArray(data)) return;

  await client.query(`CREATE TABLE IF NOT EXISTS ${resource} (payload JSONB)`);
  await client.query(`TRUNCATE ${resource}`);
  const insert = `INSERT INTO ${resource}(payload) VALUES ($1)`;
  for (const row of data) await client.query(insert, [row]);
  await client.query(
    `INSERT INTO mirror_log(resource,rowcount,synced_at)
     VALUES ($1,$2,NOW())`,
    [resource, data.length]
  );
  console.log(`✓ ${resource}: ${data.length} rows`);
}

async function main() {
  const client = await pool.connect();
  try {
    await ensureTables(client);
    const schemaResp = await fetch(`${PROXY_BASE}/schema`, {
      headers: { "x-proxy-secret": PROXY_SECRET }
    });
    const schema = await schemaResp.json();
const paths = [
  "/api/items/list",
  "/api/jobs/list",
  "/api/inventory/list",
  "/api/inventory-lots/list",
  "/api/inventory-transactions/list",
  "/api/vendors/list",
  "/api/customers/list",
  "/api/purchase-orders/list",
  "/api/sales-orders/list",
  "/api/work-orders/list",
  "/api/invoices/list",
  "/api/materials/list",
  "/api/receiving/list"
];



for (const path of paths) {
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
