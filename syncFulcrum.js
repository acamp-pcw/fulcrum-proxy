// smartFulcrumMirror.js (incremental + robust + concurrent + dependency-ordered + reporting + nested expansion)
// ============================================================================================================
// Self-aware Fulcrum data mirror with analytics and validation reporting
// - Auto-discovers /list, /bom, /routing endpoints (including nested /{id}/list)
// - Mirrors JSONB data incrementally into Postgres with concurrency and retries
// - Expands parameterised endpoints using IDs from parent tables (handles millions safely)
// - Enforces dependency-aware ordering (items → boms → jobs → inventory)
// - Generates a sync summary report (rows added, schema changes, errors)
// ============================================================================================================

import pg from "pg";
import fetch from "node-fetch";
import crypto from "crypto";
// import nodemailer from "nodemailer";

const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const PROXY_BASE   = process.env.PROXY_BASE   || "https://<YOUR_PROXY>.onrender.com";
const PROXY_SECRET = process.env.SHARED_SECRET || "<YOUR_PROXY_SECRET>";
const REPORT_EMAIL = process.env.REPORT_EMAIL || null;

//-------------------------------------------------------------
// Safe fetch with retry and exponential backoff
//-------------------------------------------------------------
async function fetchWithRetry(url, options, retries = 5, backoff = 2000) {
  for (let i = 0; i < retries; i++) {
    try {
      const resp = await fetch(url, options);
      if (resp.status === 429) {
        const retryAfter = parseInt(resp.headers.get("retry-after")) || backoff / 1000;
        console.warn(`Rate limited → waiting ${retryAfter}s`);
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
      console.warn(`Retry ${i + 1}/${retries} → ${e.message}`);
      await new Promise(r => setTimeout(r, backoff * Math.pow(2, i)));
    }
  }
}

//-------------------------------------------------------------
// Proxy JSON fetcher
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
    headers: { "x-proxy-secret": PROXY_SECRET, "content-type": "application/json" },
    body
  });
}

//-------------------------------------------------------------
// Ensure tracking tables
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
// Helper: insert big batches efficiently
//-------------------------------------------------------------
async function saveBatch(client, resource, data) {
  if (!data.length) return;
  await client.query(`CREATE TABLE IF NOT EXISTS ${resource} (payload JSONB)`);
  const insert = `INSERT INTO ${resource}(payload) VALUES ($1)`;
  const batch = 1000;
  for (let i = 0; i < data.length; i += batch) {
    const slice = data.slice(i, i + batch);
    await Promise.all(slice.map(row => client.query(insert, [row])));
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
  for (const k of keyFields) rels[k] = k.replace(/Id$/i, "").toLowerCase() + "s";
  await client.query(`
    INSERT INTO mirror_meta(table_name,key_fields,relationships,last_discovered)
    VALUES ($1,$2,$3,NOW())
    ON CONFLICT (table_name) DO UPDATE
      SET key_fields=EXCLUDED.key_fields,
          relationships=EXCLUDED.relationships,
          last_discovered=NOW();
  `,[resource, JSON.stringify([...keys]), JSON.stringify(rels)]);
}

//-------------------------------------------------------------
// Stream parent IDs
//-------------------------------------------------------------
async function* streamParentIds(client, table, batchSize = 10000) {
  let offset = 0;
  while (true) {
    const res = await client.query(
      `SELECT payload->>'id' AS id FROM ${table} ORDER BY id OFFSET $1 LIMIT $2`,
      [offset, batchSize]
    );
    if (!res.rows.length) break;
    yield res.rows.map(r => r.id).filter(Boolean);
    offset += res.rows.length;
  }
}

//-------------------------------------------------------------
// Smart resource sync with nested expansion
//-------------------------------------------------------------
async function syncResource(client, path) {
  const resource = path
    .split("/")
    .filter(p => p && !p.startsWith("{") && p !== "api")
    .join("_")
    .replace(/[^a-zA-Z0-9_]/g, "_");

  console.log(`→ syncing ${resource}`);

  const last = await client.query(
    `SELECT last_date FROM mirror_log WHERE resource=$1 ORDER BY synced_at DESC LIMIT 1`,
    [resource]
  );
  const sinceDate = last.rows[0]?.last_date || "1900-01-01";
  let total = 0;

  const param = path.match(/{(\\w+)}/);
  if (param) {
    const paramName = param[1];
    const parentTable = paramName.replace(/Id$/, "").toLowerCase() + "s";
    const exists = await client.query(
      `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=$1)`,
      [parentTable]
    );
    if (!exists.rows[0].exists) {
      console.warn(`⚠ skipping ${path}: parent ${parentTable} missing`);
      return { resource, rowcount: 0, errors: [`missing parent ${parentTable}`] };
    }

    let batchData = [];
    for await (const ids of streamParentIds(client, parentTable, 10000)) {
      for (const id of ids) {
        const fullPath = path.replace(`{${paramName}}`, id);
        try {
          const sub = await fetchJSON(fullPath, sinceDate);
          if (Array.isArray(sub) && sub.length) {
            batchData.push(...sub);
            total += sub.length;
          }
        } catch (err) {
          console.warn(`⚠ ${fullPath} → ${err.message}`);
        }
        if (batchData.length > 50000) {
          await saveBatch(client, resource, batchData);
          batchData = [];
        }
      }
    }
    await saveBatch(client, resource, batchData);
  } else {
    const data = await fetchJSON(path, sinceDate);
    await saveBatch(client, resource, data);
    total = data.length;
  }

  const hash = crypto.createHash("md5").update(String(total)).digest("hex");
  await client.query(
    `INSERT INTO mirror_log(resource,rowcount,synced_at,hash,last_date,errors)
     VALUES ($1,$2,NOW(),$3,$4,$5)`,
    [resource, total, hash, new Date().toISOString(), JSON.stringify([])]
  );
  console.log(`✓ ${resource}: ${total} rows`);
  await analyzeSchema(client, resource);
  return { resource, rowcount: total, errors: [] };
}

//-------------------------------------------------------------
// Dependency ordering
//-------------------------------------------------------------
function sortDependencies(paths) {
  const priority = ["items","item-boms","routing","jobs","workorders","inventory","materials","customers","vendors"];
  return paths.sort((a,b)=>{
    const rank = p => priority.findIndex(k=>p.includes(k));
    const ra=rank(a), rb=rank(b);
    return (ra===-1?99:ra)-(rb===-1?99:rb);
  });
}

//-------------------------------------------------------------
// Run concurrent batches
//-------------------------------------------------------------
async function runConcurrentSyncs(client, paths, batchSize=5) {
  const ordered = sortDependencies(paths);
  const results=[];
  for(let i=0;i<ordered.length;i+=batchSize){
    const slice=ordered.slice(i,i+batchSize);
    console.log(`→ batch ${i/batchSize+1}/${Math.ceil(ordered.length/batchSize)} (${slice.join(", ")})`);
    const batchResults=await Promise.all(slice.map(p=>syncResource(client,p).catch(e=>{
      console.error(`✗ ${p}: ${e.message}`); return {resource:p,rowcount:0,errors:[e.message]};
    })));
    results.push(...batchResults);
    await new Promise(r=>setTimeout(r,1000));
  }
  return results;
}

//-------------------------------------------------------------
// Build analytical views
//-------------------------------------------------------------
async function buildViews(client){
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
           (c.qty_per*j.qty_to_make) AS required_qty,
           COALESCE((inv.payload->>'onHandQuantity')::numeric,0) AS available_qty,
           (c.qty_per*j.qty_to_make-COALESCE((inv.payload->>'onHandQuantity')::numeric,0)) AS missing_qty
    FROM job_items j
    JOIN item_boms c ON c.item_id=j.item_id
    LEFT JOIN inventory inv ON inv.payload->>'itemId'=c.component_id
    WHERE (c.qty_per*j.qty_to_make-COALESCE((inv.payload->>'onHandQuantity')::numeric,0))>0;
  `);
}

//-------------------------------------------------------------
// Console summary (email disabled by default)
//-------------------------------------------------------------
async function reportResults(results){
  const total = results.reduce((s,r)=>s+r.rowcount,0);
  const failed = results.filter(r=>r.errors.length);
  console.log(`\nFulcrum Mirror Summary\n=======================\nResources: ${results.length}\nRows: ${total}\nFailures: ${failed.length}\n`);
  results.forEach(r=>{
    console.log(`• ${r.resource}: ${r.rowcount} ${r.errors.length?`errors: ${r.errors.join("; ")}`:""}`);
  });
}

//-------------------------------------------------------------
// Main orchestrator
//-------------------------------------------------------------
async function main(){
  const client=await pool.connect();
  try{
    await ensureSystemTables(client);
    console.log("Loading schema from proxy...");
    const schemaResp=await fetchWithRetry(`${PROXY_BASE}/schema`,{headers:{'x-proxy-secret':PROXY_SECRET}});
    const allPaths=(schemaResp.resources||[])
      .map(r=>r.op.path)
      .filter(p=>p.startsWith("/api/") && (/\\/list($|\\/)/.test(p) || /routing|bom/i.test(p)));
    const ordered=sortDependencies(allPaths);
    console.log(`Discovered ${ordered.length} resources (dependency ordered).`);
    const results=await runConcurrentSyncs(client,ordered,5);
    await buildViews(client);
    await reportResults(results);
    console.log("Mirror sync complete",new Date().toISOString());
  }finally{
    client.release();
    await pool.end();
  }
}

main().catch(e=>{
  console.error("Mirror job failed:",e);
  process.exit(1);
});
