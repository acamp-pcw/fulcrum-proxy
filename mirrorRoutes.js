// mirrorRoutes.js
// Read-only Express routes for mirrored data

import express from "express";
import pg from "pg";
const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const router = express.Router();

// List tables and last sync info
router.get("/summary", async (_req, res) => {
  const q = await pool.query(
    "SELECT resource, rowcount, synced_at FROM mirror_log ORDER BY resource"
  );
  res.json(q.rows);
});

// Read mirrored data (limit 500 rows)
router.get("/:resource", async (req, res) => {
  const r = req.params.resource;
  try {
    const q = await pool.query(`SELECT payload FROM ${r} LIMIT 500`);
    res.json(q.rows.map(x => x.payload));
  } catch (e) {
    res.status(404).json({ error: `Table ${r} not found` });
  }
});

// Recent logs
router.get("/logs/all", async (_req, res) => {
  const q = await pool.query(
    "SELECT * FROM mirror_log ORDER BY synced_at DESC LIMIT 50"
  );
  res.json(q.rows);
});

export default router;
