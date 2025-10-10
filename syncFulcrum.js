// smartFulcrumMirror.js (incremental + robust + concurrent + dependency-ordered + reporting)
// =========================================================================================
// Self-aware Fulcrum data mirror with analytics and validation reporting
// - Auto-discovers /list, /bom, /routing endpoints
// - Mirrors JSONB data incrementally into Postgres with concurrency and retries
// - Enforces dependency-aware ordering (items → boms → jobs → inventory)
// - Generates a sync summary report (rows added, schema changes, errors)
// =========================================================================================

import pg from "pg";
import fetch from "node-fetch";
import crypto from "crypto";
//import nodemailer from "nodemailer";

const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const PROXY_BASE = process.env.PROXY_BASE || "https://<YOUR_PROXY>.onrender.com";
const PROXY_SECRET = process.env.SHARED_SECRET || "<YOUR_PROXY_SECRET>";
const REPORT_EMAIL = process.env.REPORT_EMAIL || null;

//-------------------------------------------------------------
// Utility: safe fetch with retry and backoff
//-------------------------------------------------------------
async function fetchWithRetry(url, options, retries = 5, backoff = 2000) {
  for (let i = 0; i < retries; i++) {
    try {
      const resp = await fetch(url, options);
      if (resp.status === 429) {
        const retryAfter = parseInt(resp.headers.get('retry-after')) || backoff / 1000;
        console.warn(`Rate limited. Waiting ${retryAfter}s...`);
        await new Promise(r => setTimeout(r, retryAfter * 1000));
        continue;
      }
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`${resp.status}: ${text.slice(0, 200)}`);
      }
      return await resp.json();
    } catch (e) {
      if (i === retries - 1) throw e;
      console.warn(`Retry ${i + 1}/${retries} failed → ${e.message}`);
      await new Promise(r => setTimeout(r, backoff * Math.pow(2, i)));
    }
  }
}

//-------------------------------------------------------------
// Fetch JSON from Fulcrum proxy
//-------------------------------------------------------------
async function fetchJSON(path, sinceDate = "1900-01-01") {
  const body = JSON.stringify({
    path,
    method: "POST",
    body: { DateFrom: sinceDate },
    autoPage: { take: 500, maxPages: 100, sortField: "CreatedUtc", sortDir: "Ascending" }
  });

  return await fetchWithRetry(`${PROXY_BASE}/call`, {
    method: "POST",
    headers: {
      "x-proxy-secret": PROXY_SECRET,
      "content-type": "application/json"
    },
    body
  });
}

//-------------------------------------------------------------
// System tables for tracking sync and metadata
//-------------------------------------------------------------
async function ensureSystemTables(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS mirror_log (
      resource TEXT,
      rowcount INTEGER,
      synced_at TIMESTAMPTZ DEFAULT NOW(),
      hash TEXT,
      last_date TIMESTAMPTZ,
      errors JSONB
    );
    CREATE TABLE IF NOT EXISTS mirror_meta (
      table_name TEXT PRIMARY KEY,
      key_fields JSONB,
      relationships JSONB,
      last_discovered TIMESTAMPTZ DEFAULT NOW()
    );
  `);
}

//-------------------------------------------------------------
// Sync each resource incrementally
//-------------------------------------------------------------
//-------------------------------------------------------------
// Smarter resource sync with nested endpoint expansion
//-------------------------------------------------------------
async function syncResource(client, path) {
  // Build a unique, SQL-safe table name
  const resource = path
    .split('/')
    .filter(p => p && !p.startsWith('{') && p !== 'api')
    .join('_')
    .replace(/[^a-zA-Z0-9_]/g, '_');

  console.log(`→ syncing ${resource}`);

  // Get last sync time (for incremental updates)
  const lastLog = await client.query(
    `SELECT last_date FROM mirror_log WHERE resource=$1 ORDER BY synced_at DESC LIMIT 1`,
    [resource]
  );
  const sinceDate = lastLog.rows[0]?.last_date || '1900-01-01';

  let allData = [];
  let totalCount = 0;

  // Detect parameterized paths like {invoiceId}, {itemId}, etc.
  const paramMatch = path.match(/{(\\w+)}/);
  if (paramMatch) {
    const paramName = paramMatch[1];
    const parentTable = paramName.replace(/Id$/, '').toLowerCase() + 's';

    console.log(`→ expanding parameterized endpoint: ${path} using IDs from ${parentTable}`);

    // Confirm parent table exists
    const exists = await client.query(
      `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`,
      [parentTable]
    );
    if (!exists.rows[0].exists) {
      console.warn(`⚠ Skipping ${path} — parent table ${parentTable} not found`);
      return;
    }

    // Stream IDs in batches of 10k to support millions of parents
    const batchSize = 10000;
    let offset = 0;
    while (true) {
      const idsRes = await client.query(
        `SELECT payload->>'id' AS id FROM ${parentTable} ORDER BY id OFFSET $1 LIMIT $2`,
        [offset, batchSize]
      );
      const ids = idsRes.rows.map(r => r.id).filter(Boolean);
      if (ids.length === 0) break;

      console.log(`→ ${parentTable}: processing ${ids.length} IDs (offset ${offset})`);
      offset += ids.length;

      // Call sub-endpoint for each ID
      for (const id of ids) {
        const fullPath = path.replace(`{${paramName}}`, id);
        try {
          const subdata = await fetchJSON(fullPath, sinceDate);
          if (Array.isArray(subdata)) {
            allData.push(...subdata);
            totalCount += subdata.length;
          }
        } catch (e) {
          console.warn(`⚠ ${fullPath} failed → ${e.message}`);
        }
      }

      // Safety flush to avoid huge memory buildup
      if (allData.length > 50000) {
        await saveBatch(client, resource, allData);
        allData = [];
      }
    }
  } else {
    // Non-parameterized endpoint
    allData = await fetchJSON(path, sinceDate);
    totalCount = allData.length;
  }

  // Final flush
  if (allData.length > 0) await saveBatch(client, resource, allData);

  console.log(`✓ ${resource}: ${totalCount} total records processed`);
  await analyzeSchema(client, resource);
}

//-------------------------------------------------------------
// Helper: insert large datasets efficiently
//-------------------------------------------------------------
async function saveBatch(client, resource, data) {
  await client.query(`CREATE TABLE IF NOT EXISTS ${resource} (payload JSONB)`);
  const insert = `INSERT INTO ${resource}(payload) VALUES ($1)`;
  const batchLimit = 1000;
  for (let i = 0; i < data.length; i += batchLimit) {
    const slice = data.slice(i, i + batchLimit);
    const queries = slice.map(row => client.query(insert, [row]));
    await Promise.all(queries);
  }

  await client.query(`
    ALTER TABLE ${resource}
      ADD COLUMN IF NOT EXISTS id TEXT GENERATED ALWAYS AS (payload->>'id') STORED,
      ADD COLUMN IF NOT EXISTS createdutc TIMESTAMPTZ GENERATED ALWAYS AS (payload->>'createdUtc') STORED;
    CREATE INDEX IF NOT EXISTS ${resource}_id_idx ON ${resource}(id);
  `);
}


//-------------------------------------------------------------
// Schema analysis
//-------------------------------------------------------------
async function analyzeSchema(client, resource) {
  const sample = await client.query(`SELECT payload FROM ${resource} LIMIT 25;`);
  const keys = new Set();
  sample.rows.forEach(r => Object.keys(r.payload || {}).forEach(k => keys.add(k)));

  const keyFields = [...keys].filter(k => /id$/i.test(k));
  const rels = {};
  for (const key of keyFields) {
    const target = key.replace(/Id$/i, '').toLowerCase() + 's';
    rels[key] = target;
  }

  await client.query(`
    INSERT INTO mirror_meta(table_name, key_fields, relationships, last_discovered)
    VALUES ($1,$2,$3,NOW())
    ON CONFLICT (table_name) DO UPDATE SET
      key_fields=EXCLUDED.key_fields,
      relationships=EXCLUDED.relationships,
      last_discovered=NOW();
  `, [resource, JSON.stringify([...keys]), JSON.stringify(rels)]);
}

//-------------------------------------------------------------
// Dependency ordering
//-------------------------------------------------------------
function sortDependencies(paths) {
  const priority = ['items', 'item-boms', 'routing', 'jobs', 'workorders', 'inventory', 'materials', 'customers', 'vendors'];
  return paths.sort((a, b) => {
    const getRank = p => priority.findIndex(k => p.includes(k));
    const ra = getRank(a);
    const rb = getRank(b);
    return (ra === -1 ? 99 : ra) - (rb === -1 ? 99 : rb);
  });
}

//-------------------------------------------------------------
// Batch sync with concurrency
//-------------------------------------------------------------
async function runConcurrentSyncs(client, paths, batchSize = 5) {
  const ordered = sortDependencies(paths);
  let results = [];
  for (let i = 0; i < ordered.length; i += batchSize) {
    const batch = ordered.slice(i, i + batchSize);
    console.log(`→ Running batch ${i / batchSize + 1}/${Math.ceil(ordered.length / batchSize)} (${batch.join(', ')})`);
    const batchResults = await Promise.all(batch.map(p => syncResource(client, p)));
    results.push(...batchResults);
    await new Promise(r => setTimeout(r, 1000));
  }
  return results;
}

//-------------------------------------------------------------
// Build analytical views
//-------------------------------------------------------------
async function buildViews(client) {
  await client.query(`
    CREATE OR REPLACE VIEW job_items AS
    SELECT (payload->>'id') AS job_id,
           (payload->>'parentItemId') AS item_id,
           LOWER(payload->>'status') AS status,
           (payload->>'quantityToMake')::numeric AS qty_to_make
    FROM jobs;

    CREATE OR REPLACE VIEW item_boms AS
    SELECT (payload->>'itemId') AS item_id,
           (payload->>'componentItemId') AS component_id,
           (payload->>'quantityPer')::numeric AS qty_per
    FROM item_boms_data;

    CREATE OR REPLACE VIEW job_component_shortages AS
    SELECT j.job_id,
           j.item_id,
           c.component_id,
           (c.qty_per * j.qty_to_make) AS required_qty,
           COALESCE((inv.payload->>'onHandQuantity')::numeric,0) AS available_qty,
           (c.qty_per * j.qty_to_make - COALESCE((inv.payload->>'onHandQuantity')::numeric,0)) AS missing_qty
    FROM job_items j
    JOIN item_boms c ON c.item_id = j.item_id
    LEFT JOIN inventory inv ON inv.payload->>'itemId' = c.component_id
    WHERE (c.qty_per * j.qty_to_make - COALESCE((inv.payload->>'onHandQuantity')::numeric,0)) > 0;
  `);
}

//-------------------------------------------------------------
// Generate and optionally email summary report
//-------------------------------------------------------------
async function reportResults(results) {
  const total = results.reduce((sum, r) => sum + r.rowcount, 0);
  const failed = results.filter(r => r.errors.length > 0);

  const summary = `\nFulcrum Mirror Sync Summary\n===========================\n
Total Resources: ${results.length}\nTotal Rows Synced: ${total}\nFailures: ${failed.length}\n
` + results.map(r => `• ${r.resource}: ${r.rowcount} rows${r.errors.length ? ` (errors: ${r.errors.join('; ')})` : ''}`).join('\n');

  console.log(summary);

  if (REPORT_EMAIL) {
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: process.env.SMTP_PORT || 587,
      secure: false,
      auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
    });

    await transporter.sendMail({
      from: `Fulcrum Mirror <${process.env.SMTP_USER}>`,
      to: REPORT_EMAIL,
      subject: `Fulcrum Mirror Sync Report - ${new Date().toISOString()}`,
      text: summary
    });
    console.log(`✉ Report emailed to ${REPORT_EMAIL}`);
  }
}

//-------------------------------------------------------------
// Main orchestrator
//-------------------------------------------------------------
async function main() {
  const client = await pool.connect();
  try {
    await ensureSystemTables(client);

    console.log('Loading schema from proxy...');
    const schemaResp = await fetchWithRetry(`${PROXY_BASE}/schema`, {
      headers: { 'x-proxy-secret': PROXY_SECRET }
    });

    const allPaths = (schemaResp.resources || [])
      .map(r => r.op.path)
      .filter(p =>
  p.startsWith('/api/') &&
  (new RegExp("/list($|/)", "i").test(p) || /routing|bom/i.test(p))
);



    const orderedPaths = sortDependencies(allPaths);
    console.log(`Discovered ${orderedPaths.length} resources, dependency-ordered.`);

    const results = await runConcurrentSyncs(client, orderedPaths, 5);
    await buildViews(client);
    await reportResults(results);

    console.log('Mirror sync with reporting complete', new Date().toISOString());
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(e => {
  console.error('Mirror job failed:', e);
  process.exit(1);
});
