import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

// --- config ---
const SHARED_SECRET = process.env.SHARED_SECRET || "replace-me";
const FULCRUM_TOKEN = process.env.FULCRUM_TOKEN || "";
const ALLOWED_PREFIXES = ["/api/", "/swagger/v1/swagger.json"]; // allow proxying swagger too

// Node ≥18 built-ins
const { fetch, Headers } = globalThis;

// ------------------ /schema (cached) ------------------
let SCHEMA_CACHE = null;
let SCHEMA_CACHE_AT = 0;
const SCHEMA_TTL_MS = 24 * 60 * 60 * 1000; // 24h

async function fetchSwagger() {
  const url = "https://api.fulcrumpro.com/swagger/v1/swagger.json";
  const r = await fetch(url, {
    headers: new Headers({ Authorization: `Bearer ${FULCRUM_TOKEN}` })
  });
  if (!r.ok) throw new Error(`Swagger fetch failed: ${r.status}`);
  return r.json();
}

function compileCatalog(sw) {
  const catalog = { resources: [], enums: {}, version: sw.info?.version || "unknown", hints: {} };
  const schemas = sw.components?.schemas || {};
  for (const [name, sch] of Object.entries(schemas)) {
    if (sch?.enum && Array.isArray(sch.enum)) catalog.enums[name] = sch.enum;
  }
  const paths = sw.paths || {};
  for (const [p, ops] of Object.entries(paths)) {
    for (const [m, def] of Object.entries(ops)) {
      const method = m.toUpperCase();
      const seg = p.split("/").filter(Boolean); // ["api","jobs","list"]
      const resource = seg[1] || "root";
      catalog.resources.push({
        resource,
        op: {
          path: p,
          method,
          operationId: def.operationId || `${resource}_${method}`,
          summary: def.summary || "",
          isList: /\/list$/.test(p),
          isGetById: /{\w+}$/.test(p),
          acceptsBody: !!def.requestBody,
          hasQuery: true
        }
      });
    }
  }
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

app.get("/schema", async (_req, res) => {
  try {
    const cat = await getCatalog();
    res.json(cat);
  } catch (e) {
    console.error("Schema error:", e);
    res.status(500).json({ error: String(e) });
  }
});

// ------------------ proxy endpoint ------------------
app.post("/call", async (req, res) => {
  try {
    const { method, path, query = {}, headers = {}, body, secret } = req.body || {};
console.log(new Date().toISOString(), "CALL", {
  path,
  method,
  hasBody: !!body,
  hasQuery: !!query,
  gotHeaderSecret: !!req.headers["x-proxy-secret"],
  gotBodySecret: !!(body && body.secret)
});

    // NEW: accept header-based secret (preferred), fall back to body.secret
// accept header or body secret
const headerSecret = req.headers["x-proxy-secret"];
const providedSecret = headerSecret || (req.body && req.body.secret);
if (providedSecret !== SHARED_SECRET) {
  return res.status(401).json({ error: "invalid_proxy_secret" });
}


    // ... keep the rest unchanged ...


    // allowlist check (MUST be inside the route handler)
    if (!path || !ALLOWED_PREFIXES.some(pref => path.startsWith(pref))) {
      return res.status(400).json({ error: "Path not allowed" });
    }

    // build URL
    const qs = Object.keys(query).length ? "?" + new URLSearchParams(query).toString() : "";
    const url = `https://api.fulcrumpro.com${path}${qs}`;

    // headers
    const fwdHeaders = new Headers({
      Authorization: `Bearer ${FULCRUM_TOKEN}`,
      "Content-Type": headers["content-type"] || "application/json"
    });

    // method inference & body handling
    const hasBody = body && Object.keys(body).length > 0;
    const methodUp = (method || (hasBody ? "POST" : "GET")).toUpperCase();

    const resp = await fetch(url, {
      method: methodUp,
      headers: fwdHeaders,
      body: methodUp === "GET" ? undefined : (hasBody ? JSON.stringify(body) : undefined)
    });

    // normalize upstream auth errors
    if (resp.status === 401 || resp.status === 403) {
      const text = await resp.text();
      let upstream;
      try { upstream = JSON.parse(text); } catch { upstream = { raw: text }; }
      return res.status(resp.status).json({ error: "fulcrum_unauthorized", upstream });
    }

    // buffer response (WebStream → Buffer) and pass through
    const contentType = resp.headers.get("content-type") || "application/json";
    const ab = await resp.arrayBuffer();
    const buf = Buffer.from(ab);

    res.status(resp.status);
    res.set("content-type", contentType);
    res.send(buf);
  } catch (e) {
    console.error("Proxy error:", e);
    res.status(500).json({ error: String(e) });
  }
});

// ------------------ health ------------------
app.get("/healthz", (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 3000;

app.post("/diag", (req, res) => {
  const headerSecret = req.headers["x-proxy-secret"];
  const bodySecret = req.body && req.body.secret;
  res.json({
    ok: true,
    gotHeader: !!headerSecret,
    gotBody: !!bodySecret,
    headerMatches: headerSecret === SHARED_SECRET,
    bodyMatches: bodySecret === SHARED_SECRET
  });
});

// log every /call attempt (put at top of the /call handler)
app.post("/call", async (req, res) => {
  const { method, path, query = {}, body } = req.body || {};
  console.log(new Date().toISOString(), "CALL",
    { path, method, hasBody: !!body, hasQuery: !!query,
      gotHeaderSecret: !!req.headers["x-proxy-secret"],
      gotBodySecret: !!(body && body.secret) });

  // ... keep your existing secret check & proxy code here ...
});

app.post("/diag", (req, res) => {
  const headerSecret = req.headers["x-proxy-secret"];
  const bodySecret   = req.body && req.body.secret;
  res.json({
    ok: true,
    gotHeader: !!headerSecret,
    gotBody: !!bodySecret,
    headerMatches: headerSecret === (process.env.SHARED_SECRET || "replace-me"),
    bodyMatches: bodySecret === (process.env.SHARED_SECRET || "replace-me")
  });
});

app.listen(port, () => console.log("Proxy running on", port));
