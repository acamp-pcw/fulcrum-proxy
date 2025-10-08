import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

const ALLOWED_PATHS = ["/api/", "/swagger/v1/swagger.json"];
// ...
if (!path || !ALLOWED_PATHS.some(p => path.startsWith(p))) {
  return res.status(400).json({ error: "Path not allowed" });
}
const SHARED_SECRET = process.env.SHARED_SECRET || "replace-me";
const { fetch, Headers } = globalThis;

app.post("/call", async (req, res) => {
  try {
    const { method, path, query = {}, headers = {}, body, secret } = req.body || {};

    if (!path || !path.startsWith(ALLOWED_PREFIX)) {
      return res.status(400).json({ error: "Path not allowed" });
    }
    if (secret !== SHARED_SECRET) {
      return res.status(401).json({ error: "Invalid secret" });
    }

    const qs = Object.keys(query).length ? "?" + new URLSearchParams(query).toString() : "";
    const url = `https://api.fulcrumpro.com${path}${qs}`;

    const fwdHeaders = new Headers({
      Authorization: `Bearer ${process.env.FULCRUM_TOKEN}`,
      "Content-Type": headers["content-type"] || "application/json"
    });

    const resp = await fetch(url, {
      method: (method || "GET").toUpperCase(),
      headers: fwdHeaders,
      body: body ? JSON.stringify(body) : undefined
    });

    // ---- Buffered pass-through (works with Web streams) ----
    const contentType = resp.headers.get("content-type") || "application/json";
    const ab = await resp.arrayBuffer();                 // <— buffer the body
    const buf = Buffer.from(ab);

    res.status(resp.status);
    res.set("content-type", contentType);
    res.send(buf);
    // --------------------------------------------------------

  } catch (e) {
    console.error("Proxy error:", e);
    res.status(500).json({ error: String(e) });
  }
});

app.get("/healthz", (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 3000;

// --- Schema compiler cache ---
let SCHEMA_CACHE = null;
let SCHEMA_CACHE_AT = 0;
const SCHEMA_TTL_MS = 24 * 60 * 60 * 1000; // 24h

async function fetchSwagger() {
  const url = "https://api.fulcrumpro.com/swagger/v1/swagger.json";
  const r = await fetch(url, {
    headers: new Headers({
      Authorization: `Bearer ${process.env.FULCRUM_TOKEN}`
    })
  });
  if (!r.ok) throw new Error(`Swagger fetch failed: ${r.status}`);
  return r.json();
}

// Build a compact catalog the GPT can reason over quickly.
function compileCatalog(sw) {
  const catalog = { resources: [], enums: {}, version: sw.info?.version || "unknown" };

  // Enums from components.schemas (best effort)
  const schemas = sw.components?.schemas || {};
  for (const [name, sch] of Object.entries(schemas)) {
    if (sch?.enum && Array.isArray(sch.enum)) {
      catalog.enums[name] = sch.enum;
    }
  }

  // Paths → resources
  const paths = sw.paths || {};
  for (const [p, ops] of Object.entries(paths)) {
    for (const [m, def] of Object.entries(ops)) {
      const method = m.toUpperCase();
      // Normalize resource name (jobs, invoices, items, inventory, customers, etc.)
      const seg = p.split("/").filter(Boolean);
      const resource = seg[1] || "root"; // e.g., /api/jobs/list -> "jobs"
      const op = {
        path: p,
        method,
        operationId: def.operationId || `${resource}_${method}`,
        summary: def.summary || "",
        // quick hints for planner
        isList: /\/list$/.test(p),
        isGetById: /{\w+}$/.test(p),
        acceptsBody: !!def.requestBody,
        hasQuery: true
      };
      catalog.resources.push({ resource, op });
    }
  }

  // Heuristic hints the planner can use
  catalog.hints = {
    jobs: { prefer: ["/api/jobs/list", "/api/work-orders/list", "/api/production-jobs/list"] },
    inventory: {
      availability: "/api/inventory/availableByItem",
      byItem: "/api/inventory/byItem",
      transactions: "/api/inventory-transactions/list",
      lots: "/api/inventory-lots/list"
    },
    items: { list: "/api/items/list/v2" },
    invoices: { list: "/api/invoices/list" },
    customers: { list: "/api/customers/list" }
  };

  return catalog;
}

async function getCatalog() {
  const now = Date.now();
  if (SCHEMA_CACHE && now - SCHEMA_CACHE_AT < SCHEMA_TTL_MS) return SCHEMA_CACHE;
  const sw = await fetchSwagger();
  SCHEMA_CACHE = compileCatalog(sw);
  SCHEMA_CACHE_AT = now;
  return SCHEMA_CACHE;
}

// Expose to GPT (no secret required beyond your proxy secret on /call)
app.get("/schema", async (_req, res) => {
  try {
    const cat = await getCatalog();
    res.json(cat);
  } catch (e) {
    console.error("Schema error:", e);
    res.status(500).json({ error: String(e) });
  }
});


app.listen(port, () => console.log("Proxy running on", port));
