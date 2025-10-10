import pg from "pg";
const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const sql = `
  ALTER TABLE mirror_log
    ADD COLUMN IF NOT EXISTS last_date TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS errors JSONB;
`;

const run = async () => {
  const client = await pool.connect();
  try {
    await client.query(sql);
    console.log("✅ Migration complete");
  } catch (err) {
    console.error("❌ Migration failed:", err.message);
  } finally {
    client.release();
    await pool.end();
  }
};

run();
