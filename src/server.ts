import express from "express";
import multer from "multer";
import * as DuckDB from "duckdb";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import cors from "cors";
import * as path from "path";
import * as fs from "fs";
import { pipeline } from "stream/promises";

const app = express();

const uploadsDir = path.resolve("uploads");
fs.mkdirSync(uploadsDir, { recursive: true });

function getS3Config() {
  const bucket = process.env.S3_BUCKET;
  const region = process.env.S3_REGION;
  if (!bucket || !region) return null;

  const endpoint = process.env.S3_ENDPOINT;
  const forcePathStyle = process.env.S3_FORCE_PATH_STYLE === "true";

  return { bucket, region, endpoint, forcePathStyle };
}

function getS3Client(): { client: S3Client; bucket: string } | null {
  const cfg = getS3Config();
  if (!cfg) return null;

  const client = new S3Client({
    region: cfg.region,
    endpoint: cfg.endpoint || undefined,
    forcePathStyle: cfg.forcePathStyle,
  });

  return { client, bucket: cfg.bucket };
}

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

function jsonSafeValue(value: any): any {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(jsonSafeValue);
  if (value && typeof value === "object") {
    const ctorName = value?.constructor?.name;
    if (ctorName && ctorName !== "Object" && ctorName !== "Array") {
      const str = typeof value.toString === "function" ? value.toString() : "";
      if (str && str !== "[object Object]") return str;
    }

    const entries = Object.entries(value);
    if (entries.length === 0) {
      const str = typeof value.toString === "function" ? value.toString() : "";
      if (str && str !== "[object Object]") return str;
      return null;
    }

    const out: Record<string, any> = {};
    for (const [k, v] of entries) out[k] = jsonSafeValue(v);
    return out;
  }
  return value;
}

function queryDuckDB(sql: string): Promise<{ columns: string[]; rows: any[][] }> {
  return new Promise((resolve, reject) => {
    db.all(sql, (err: Error | null, rows: any[]) => {
      if (err) return reject(err);
      if (rows.length === 0) return resolve({ columns: [], rows: [] });

      const columns = Object.keys(rows[0]);
      const rowArrays = rows.map((r) => columns.map((c) => jsonSafeValue(r[c])));
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
    const rowCount = jsonSafeValue(count.rows[0][0]);

    fs.unlink(csvPath, () => {});

    res.json({ rowCount });
  } catch (err: any) {
    console.error(err);
    res.status(500).send(err.message ?? "Internal error");
  }
});

app.post("/upload/presign", async (req, res) => {
  const { filename, contentType } = req.body as { filename?: string; contentType?: string };
  const s3 = getS3Client();
  if (!s3) {
    return res.status(501).type("text/plain").send("S3 not configured");
  }

  const safeExt = filename ? path.extname(filename) : ".csv";
  const key = `uploads/${Date.now()}-${Math.random().toString(16).slice(2)}${safeExt || ".csv"}`;

  try {
    const cmd = new PutObjectCommand({
      Bucket: s3.bucket,
      Key: key,
      ContentType: contentType || "text/csv",
    });
    const uploadUrl = await getSignedUrl(s3.client, cmd, { expiresIn: 3600 });
    res.json({ uploadUrl, key });
  } catch (err: any) {
    console.error(err);
    res.status(500).type("text/plain").send(err.message ?? "Failed to presign upload");
  }
});

app.post("/upload/import", async (req, res) => {
  const { key } = req.body as { key?: string };
  const s3 = getS3Client();
  if (!s3) {
    return res.status(501).type("text/plain").send("S3 not configured");
  }
  if (!key) {
    return res.status(400).type("text/plain").send("Missing key");
  }

  const localPath = path.join(
    uploadsDir,
    `s3-${Date.now()}-${Math.random().toString(16).slice(2)}.csv`
  );

  try {
    const obj = await s3.client.send(new GetObjectCommand({ Bucket: s3.bucket, Key: key }));
    if (!obj.Body) {
      return res.status(500).type("text/plain").send("Missing object body");
    }

    await pipeline(obj.Body as any, fs.createWriteStream(localPath));

    await queryDuckDB("DROP TABLE IF EXISTS tablename;");
    await queryDuckDB(`
      CREATE TABLE tablename AS
      SELECT * FROM read_csv_auto('${localPath}', HEADER=TRUE);
    `);

    const count = await queryDuckDB("SELECT COUNT(*) AS cnt FROM tablename;");
    const rowCount = jsonSafeValue(count.rows[0][0]);

    fs.unlink(localPath, () => {});
    res.json({ rowCount });
  } catch (err: any) {
    console.error(err);
    fs.unlink(localPath, () => {});
    res.status(500).type("text/plain").send(err.message ?? "Import failed");
  }
});

app.post("/query", async (req, res) => {
  const { query, page, pageSize } = req.body as {
    query?: string;
    page?: number;
    pageSize?: number;
  };
  if (!query) {
    return res.status(400).send("Missing query");
  }

  try {
    const trimmed = query.trim().replace(/;\s*$/, "");

    const safePageSizeRaw = typeof pageSize === "number" ? pageSize : 100;
    const safePageSize = Math.max(1, Math.min(5000, Math.floor(safePageSizeRaw)));
    const safePageRaw = typeof page === "number" ? page : 1;
    const safePage = Math.max(1, Math.floor(safePageRaw));
    const offset = (safePage - 1) * safePageSize;

    const sql = `SELECT * FROM (${trimmed}) AS q LIMIT ${safePageSize + 1} OFFSET ${offset}`;
    const result = await queryDuckDB(sql);

    const hasNext = result.rows.length > safePageSize;
    const rows = hasNext ? result.rows.slice(0, safePageSize) : result.rows;

    res.json({
      columns: result.columns,
      rows,
      page: safePage,
      pageSize: safePageSize,
      hasNext,
    });
  } catch (err: any) {
    console.error(err);
    res.status(400).send(`Query error: ${err.message}`);
  }
});

app.use((err: any, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  if (err?.code === "LIMIT_FILE_SIZE") {
    return res.status(413).type("text/plain").send("File too large (max 1GB)");
  }
  if (err) {
    console.error(err);
    return res.status(500).type("text/plain").send(err.message ?? "Internal error");
  }
});

app.get("/debug/routes", (_req, res) => {
  const router = (app as any).router;
  const stack: any[] = router?.stack ?? [];
  const routes = stack
    .map((layer) => {
      const route = layer?.route;
      if (!route) return null;
      const methods = Object.keys(route.methods ?? {}).filter((m) => route.methods[m]);
      return { path: route.path, methods };
    })
    .filter(Boolean);
  res.json({ routes });
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Backend listening on port ${port}`);
});
