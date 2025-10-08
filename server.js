// server.js (ESM, Node >= 20)
import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

// ---------- config ----------
const SHARED_SECRET   = process.env.SHARED_SECRET  || "replace-me";
const FULCRUM_TOKEN   = process.env.FULCRUM_TOKEN  || "";
const ALLOWED_PREFIXES = ["/api/", "/swagger/v1/swagger.json"]; // outbound allowlist

// Node >=18 globals
const { fetch, Headers } = globalThis;

// ---------- optional: /schema (cached swagger -> compact catalog) ----------
let SCHEMA_CACHE = null;
let SCHEMA_CACHE_AT = 0;
const SCHEMA_TTL_MS = 24 * 60 * 60 * 1000; // 24h

async function fetchSwagger() {
  const r = await fetch("https://api.fulcrumpro.com/swagger/v1/swagger.json", {
    headers: new Headers({ Authorization: `Bearer ${FULCRUM_TOKEN}` })
  });
  if (!r.ok) throw new Error(`Swagger fetch failed: ${r.status}`);
  return r.json();
}

function compileCatalog(sw) {
  const catalog = { resources: [], enums: {}, version: sw.info?.version || "unknown", hints: {} };
  const schemas = sw.components?.schemas || {};
  for (const [name, sch] of Object.entries(schemas)) {
    if (Array.isArray(sch?.enum)) catalog.enums[name] = sch.enum;
  }
  const paths = sw.paths || {};
  for (const [p, ops] of Object.entries(paths)) {
    for (const [m, def] of Object.entries(ops)) {
      const seg = p.split("/").filter(Boolean); // ["api","jobs","list"]
      catalog.resources.push({
        resource: seg[1] || "root",
        op: {
          path: p,
          method: (m || "").toUpperCase(),
          summary: def?.summary || "",
          isList: /\/list$/.test(p),
          acceptsBody: !!def?.requestBody
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
    res.json(await getCatalog());
  } catch (e) {
    console.error("Schema error:", e);
    res.status(500).json({ error: String(e) });
  }
});

// ---------- helper: single page fetch ----------
async function fetchPage({ url, methodUp, headers, body }) {
  const resp = await fetch(url, { method: methodUp, headers, body });
  // Normalize auth errors early
  if (resp.status === 401 || resp.status === 403) {
    const text = await resp.text();
    let upstream;
    try { upstream = JSON.parse(text); } catch { upstream = { raw: text }; }
    const err = new Error("fulcrum_unauthorized");
    err.status = resp.status;
    err.upstream = upstream;
    throw err;
  }
  let text = await resp.text();
  if (!text || text.trim() === "") text = "[]"; // lists should never be empty body
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  return { status: resp.status, data };
}

// ---------- proxy endpoint (single, hardened, auto-paginate) ----------
app.post("/call", async (req, res) => {
  const started = Date.now();
  try {
    const {
      method,
      path,
      query = {},
      headers = {},
      body,
      secret,
      autoPage // { take?:number, maxPages?:number, maxRows?:number, sortField?:string, sortDir?: "Asc"|"Desc", startSkip?: number }
    } = req.body || {};

    // concise request log
    console.log(new Date().toISOString(), "CALL", {
      path, method,
      hasBody: !!body,
      gotHeaderSecret: !!req.headers["x-proxy-secret"],
      gotBodySecret: !!secret,
      autoPage: !!autoPage
    });

    // header secret preferred, body fallback
    const headerSecret   = req.headers["x-proxy-secret"];
    const providedSecret = headerSecret || secret;
    if (providedSecret !== SHARED_SECRET) {
      return res.status(401).json({ error: "invalid_proxy_secret" });
    }

    // allowlist
    if (!path || !ALLOWED_PREFIXES.some(p => path.startsWith(p))) {
      return res.status(400).json({ error: "Path not allowed" });
    }

    // determine method & safe body (GPT-proof)
    const isList  = typeof path === "string" && /\/list(?:$|\?)/.test(path);
    const hasBody = body && typeof body === "object" && Object.keys(body).length > 0;
    const methodUp = (method
      ? method.toUpperCase()
      : (isList ? "POST" : (hasBody ? "POST" : "GET")));

    // Upstream headers — always declare Accept
    const fwdHeaders = new Headers({
      Authorization: `Bearer ${FULCRUM_TOKEN}`,
      "Content-Type": headers?.["content-type"] || "application/json",
      Accept: "application/json",
      "User-Agent": "fulcrum-proxy/1.0"
    });

    // Build base query string
    const baseQS = new URLSearchParams(query);
    // Default sort if autoPage requested and none provided
    if (autoPage?.sortField && !baseQS.has("Sort.Field")) baseQS.set("Sort.Field", autoPage.sortField);
    if (autoPage?.sortDir && !baseQS.has("Sort.Dir"))     baseQS.set("Sort.Dir", autoPage.sortDir);

    const baseUrl = `https://api.fulcrumpro.com${path}`;

    // Ensure non-empty body for /list endpoints (Fulcrum can return blank on empty POST)
    const defaultListBody = { DateFrom: "1900-01-01" };
    const finalBodyBase =
      methodUp === "GET" ? undefined :
      hasBody ? JSON.stringify(body) :
      isList  ? JSON.stringify(defaultListBody) :
                "{}";

    // If no autoPage or not a list, do a single request
    if (!isList || !autoPage) {
      const qs = baseQS.toString();
      const url = qs ? `${baseUrl}?${qs}` : baseUrl;
      const { status, data } = await fetchPage({ url, methodUp, headers: fwdHeaders, body: finalBodyBase });
      return res.status(status).json(data);
    }

    // ---- Auto-pagination for list endpoints ----
    const take      = Math.max(1, Math.min( Number(autoPage.take ?? query?.Take ?? 200), 500 ));
    const maxPages  = Math.max(1, Math.min( Number(autoPage.maxPages ?? 10), 100 ));
    const maxRows   = Math.max(1, Math.min( Number(autoPage.maxRows ?? 2000), 100000 ));
    let skip        = Number(autoPage.startSkip ?? query?.Skip ?? 0);

    const all = [];
    for (let page = 0; page < maxPages && all.length < maxRows; page++) {
      baseQS.set("Skip", String(skip));
      baseQS.set("Take", String(take));
      const qs = baseQS.toString();
      const url = qs ? `${baseUrl}?${qs}` : baseUrl;

      const { status, data } = await fetchPage({ url, methodUp, headers: fwdHeaders, body: finalBodyBase });
      if (status < 200 || status >= 300) {
        return res.status(status).json(data);
      }

      if (Array.isArray(data)) {
        all.push(...data);
        if (data.length < take) break; // last page
      } else if (data && Array.isArray(data.items)) {
        all.push(...data.items);
        if (data.items.length < take) break;
      } else {
        // Unexpected shape; return what we got
        return res.status(200).json(data);
      }

      skip += take;
      if (all.length >= maxRows) break;
    }

    return res.status(200).json(all);

  } catch (e) {
    const ms = Date.now() - started;
    console.error("Proxy error:", e);
    if (e?.status === 401 || e?.status === 403) {
      return res.status(e.status).json({ error: "fulcrum_unauthorized", upstream: e.upstream });
    }
    return res.status(500).json({ error: "proxy_error", detail: String(e), elapsedMs: ms });
  }
});

// ---------- health ----------
app.get("/healthz", (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log("Proxy running on", port));
