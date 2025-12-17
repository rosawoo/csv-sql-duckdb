import express from "express";
import multer from "multer";
import * as DuckDB from "duckdb";
import cors from "cors";
import * as path from "path";
import * as fs from "fs";

const app = express();

const uploadsDir = path.resolve("uploads");
fs.mkdirSync(uploadsDir, { recursive: true });

const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, uploadsDir),
    filename: (_req, file, cb) => {
      const safeExt = path.extname(file.originalname || "");
      cb(null, `${Date.now()}-${Math.random().toString(16).slice(2)}${safeExt}`);
    },
  }),
  limits: {
    fileSize: 1024 * 1024 * 1024,
  },
});

// Allow your Vercel domain in production; * is fine for the assignment demo
app.use(cors({ origin: "*"}));
app.use(express.json());

const publicDir = path.resolve("public");
app.use(express.static(publicDir));

const db = new DuckDB.Database("data.duckdb");

function queryDuckDB(sql: string): Promise<{ columns: string[]; rows: any[][] }> {
  return new Promise((resolve, reject) => {
    db.all(sql, (err: Error | null, rows: any[]) => {
      if (err) return reject(err);
      if (rows.length === 0) return resolve({ columns: [], rows: [] });

      const columns = Object.keys(rows[0]);
      const rowArrays = rows.map((r) => columns.map((c) => r[c]));
      resolve({ columns, rows: rowArrays });
    });
  });
}

async function hasTable(): Promise<boolean> {
  try {
    const result = await queryDuckDB(
      "SELECT 1 FROM information_schema.tables WHERE table_name = 'tablename' LIMIT 1;"
    );
    return result.rows.length > 0;
  } catch {
    return false;
  }
}

app.get("/health", async (_req, res) => {
  res.json({ ok: true, duckdb: "ok", hasTable: await hasTable() });
});

app.get("/", (_req, res) => {
  res.sendFile(path.join(publicDir, "index.html"));
});

app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).send("No file uploaded");

    const csvPath = path.resolve(req.file.path);

    await queryDuckDB("DROP TABLE IF EXISTS tablename;");

    await queryDuckDB(`
      CREATE TABLE tablename AS
      SELECT * FROM read_csv_auto('${csvPath}', HEADER=TRUE);
    `);

    const count = await queryDuckDB("SELECT COUNT(*) AS cnt FROM tablename;");
    const rowCount = count.rows[0][0];

    fs.unlink(csvPath, () => {});

    res.json({ rowCount });
  } catch (err: any) {
    console.error(err);
    res.status(500).send(err.message ?? "Internal error");
  }
});

app.post("/query", async (req, res) => {
  const { query, noLimit } = req.body as { query?: string; noLimit?: boolean };
  if (!query) {
    return res.status(400).send("Missing query");
  }

  try {
    const trimmed = query.trim().replace(/;\s*$/, "");
    const sql = noLimit ? trimmed : `SELECT * FROM (${trimmed}) AS q LIMIT 1000`;
    const result = await queryDuckDB(sql);
    res.json(result);
  } catch (err: any) {
    console.error(err);
    res.status(400).send(`Query error: ${err.message}`);
  }
});

app.use((err: any, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  if (err?.code === "LIMIT_FILE_SIZE") {
    return res.status(413).send("File too large (max 1GB)");
  }
  if (err) {
    console.error(err);
    return res.status(500).send(err.message ?? "Internal error");
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Backend listening on port ${port}`);
});
