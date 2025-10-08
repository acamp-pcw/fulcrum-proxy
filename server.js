import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

// --- config ---
const SHARED_SECRET   = process.env.SHARED_SECRET  || "replace-me";
const FULCRUM_TOKEN   = process.env.FULCRUM_TOKEN  || "";
const ALLOWED_PREFIXES = ["/api/", "/swagger/v1/swagger.json"]; // allow swagger too

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

// ------------------ proxy endpoint (single, robust) ------------------
app.post("/call", async (req, res) => {
  try {
    const { method, path, query = {}, headers = {}, body, secret } = req.body || {};

    // request log
    console.log(new Date().toISOString(), "CALL", {
      path,
      method,
      hasBody: !!body,
      hasQuery: !!query,
      gotHeaderSecret: !!req.headers["x-proxy-secret"],
      gotBodySecret: !!secret
    });

    // header or body secret
    const headerSecret   = req.headers["x-proxy-secret"];
    const providedSecret = headerSecret || secret;
    if (providedSecret !== SHARED_SECRET) {
      return res.status(401).json({ error: "invalid_proxy_secret" });
    }

    // ✅ INTERNAL DIAG handled here BEFORE the allowlist
    if (path === "/diag") {
      return res.json({
        ok: true,
        gotHeader: !!headerSecret,
        gotBody: !!secret,
        headerMatches: headerSecret === SHARED_SECRET,
        bodyMatches: secret === SHARED_SECRET
      });
    }

    // allowlist (Fulcrum + swagger only)
    if (!path || !ALLOWED_PREFIXES.some(pref => path.startsWith(pref))) {
      return res.status(400).json({ error: "Path not allowed" });
    }

    // build URL
    const qs  = Object.keys(query).length ? "?" + new URLSearchParams(query).toString() : "";
    const url = `https://api.fulcrumpro.com${path}${qs}`;

    // headers to Fulcrum
    const fwdHeaders = new Headers({
      Authorization: `Bearer ${FULCRUM_TOKEN}`,
      "Content-Type": headers["content-type"] || "application/json",
      "User-Agent": "fulcrum-proxy/1.0"
    });

    // method inference & body
    const hasBody = body && Object.keys(body).length > 0;
    const methodUp = (method || (hasBody ? "POST" : "GET")).toUpperCase();

    const resp = await fetch(url, {
      method: methodUp,
      headers: fwdHeaders,
      body: methodUp === "GET" ? undefined : (hasBody ? JSON.stringify(body) : undefined)
    });

    if (resp.status === 401 || resp.status === 403) {
      const text = await resp.text();
      let upstream; try { upstream = JSON.parse(text); } catch { upstream = { raw: text }; }
      return res.status(resp.status).json({ error: "fulcrum_unauthorized", upstream });
    }

    // always return JSON (connector-friendly)
    const text = await resp.text();
    let data; try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
    return res.status(resp.status).json(data ?? {});
  } catch (e) {
    console.error("Proxy error:", e);
    return res.status(500).json({ error: "proxy_error", detail: String(e) });
  }
});

// ------------------ health ------------------
app.get("/healthz", (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log("Proxy running on", port));
