import express, { Request, Response } from "express";
import postgres from "postgres";

const app = express();
const PORT = parseInt(process.env.PORT ?? "3001", 10);
const SERVICE_NAME = "cynthiaos-ingestion-worker";

app.use(express.json());

// ── Database client (singleton) ───────────────────────────────────────────────
function getDb(): postgres.Sql {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL environment variable is not set");
  }
  return postgres(databaseUrl, { ssl: "require", max: 5, idle_timeout: 30 });
}

// ── Database connectivity state ───────────────────────────────────────────────
let dbConnected = false;
let dbTimestamp: string | null = null;

async function checkDatabaseConnectivity(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    console.log(`[${SERVICE_NAME}] DATABASE_URL not set — skipping DB check`);
    return;
  }
  try {
    const sql = getDb();
    const result = await sql`SELECT NOW() AS now`;
    dbTimestamp = result[0].now.toISOString();
    dbConnected = true;
    console.log(`[${SERVICE_NAME}] DB connectivity verified — SELECT NOW() = ${dbTimestamp}`);
    await sql.end();
  } catch (err) {
    console.error(`[${SERVICE_NAME}] DB connectivity check FAILED:`, err);
    dbConnected = false;
  }
}

// ── Job lifecycle functions ───────────────────────────────────────────────────

interface IngestionJob {
  id: string;
  job_type: string;
  trigger_type: string;
  status: string;
  started_at: Date;
  completed_at: Date | null;
  created_at: Date;
}

async function createIngestionJob(
  sql: postgres.Sql,
  jobType: string,
  triggerType: string
): Promise<IngestionJob> {
  const now = new Date();
  const rows = await sql<IngestionJob[]>`
    INSERT INTO ingestion_jobs (job_type, trigger_type, status, started_at, created_at)
    VALUES (${jobType}, ${triggerType}, 'started', ${now}, ${now})
    RETURNING *
  `;
  const job = rows[0];
  console.log(`[${SERVICE_NAME}] createIngestionJob — id=${job.id} type=${job.job_type} status=${job.status}`);
  return job;
}

async function completeIngestionJob(
  sql: postgres.Sql,
  jobId: string
): Promise<IngestionJob> {
  const now = new Date();
  const rows = await sql<IngestionJob[]>`
    UPDATE ingestion_jobs
    SET status = 'completed', completed_at = ${now}
    WHERE id = ${jobId}
    RETURNING *
  `;
  if (rows.length === 0) {
    throw new Error(`Job not found: ${jobId}`);
  }
  const job = rows[0];
  console.log(`[${SERVICE_NAME}] completeIngestionJob — id=${job.id} status=${job.status} completed_at=${job.completed_at}`);
  return job;
}

// ── POST /jobs/start ──────────────────────────────────────────────────────────
app.post("/jobs/start", async (req: Request, res: Response) => {
  const jobType: string = req.body?.job_type ?? "manual";
  const triggerType: string = req.body?.trigger_type ?? "api";

  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();

    // Create job
    const job = await createIngestionJob(sql, jobType, triggerType);

    // Immediately mark completed (test lifecycle only — no external ingestion)
    const completedJob = await completeIngestionJob(sql, job.id);

    res.status(200).json({
      success: true,
      job: {
        id: completedJob.id,
        job_type: completedJob.job_type,
        trigger_type: completedJob.trigger_type,
        status: completedJob.status,
        started_at: completedJob.started_at,
        completed_at: completedJob.completed_at,
        created_at: completedJob.created_at,
      },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[${SERVICE_NAME}] POST /jobs/start error:`, message);
    res.status(500).json({ success: false, error: message });
  } finally {
    if (sql) await sql.end();
  }
});

// ── Health check ──────────────────────────────────────────────────────────────
app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({
    service: SERVICE_NAME,
    status: "ok",
    timestamp: new Date().toISOString(),
    db: {
      connected: dbConnected,
      verified_at: dbTimestamp,
    },
  });
});

// ── Catch-all ─────────────────────────────────────────────────────────────────
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: "not_found" });
});

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", async () => {
  console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
  await checkDatabaseConnectivity();
});

export default app;
