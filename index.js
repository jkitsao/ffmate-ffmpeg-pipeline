import fs from "fs";
import path from "path";
import os from "os";
import { execFileSync } from "child_process";
import { pipeline } from "node:stream/promises";
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3"; // npm install @aws-sdk/client-s3
require("dotenv").config();
/* ============================================================================
 * OTT TRANSCODING WORKER  (Directus media_jobs + ffmate + Cloudflare R2)
 *
 *   poll media_jobs (queued)  ->  claim (downloading)  ->  pull source from R2
 *     ->  transcoding (ladder)  ->  packaging (HLS)  ->  uploading (R2)
 *     ->  write content_catalog.hls_url  ->  completed     (or failed + error)
 *
 * Directus is the source of truth for every step. The encoding internals
 * (probe / no-upscale ladder / keyframe-aligned encodes / stream-copy HLS)
 * are unchanged — only the orchestration around them is new.
 * ==========================================================================*/

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------
const BASE_URL = process.env.FFMATE_URL;

// ffmate task webhook (optional). See submitTask() for where it's attached.
const FFMATE_WEBHOOK_URL = process.env.FFMATE_WEBHOOK_URL || "";
// task.updated fires on every status/progress change — best for live tracking.
const FFMATE_WEBHOOK_EVENT = process.env.FFMATE_WEBHOOK_EVENT || "task.updated";

// Directus
const DIRECTUS_URL = (process.env.DIRECTUS_URL || "").replace(/\/+$/, "");
const DIRECTUS_TOKEN =
  process.env.DIRECTUS_TOKEN || process.env.DIRECTUS_SERVER_TOKEN || "";

// Worker
const ENCODER_NODE = process.env.ENCODER_NODE || os.hostname();
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 3000);
const CLEANUP_AFTER = process.env.CLEANUP_AFTER === "1";

// media_jobs status vocabulary (must match the Directus field's allowed values)
const STATUS = {
  QUEUED: "queued", // written by the app on upload (attachMedia)
  DOWNLOADING: "downloading",
  TRANSCODING: "transcoding",
  PACKAGING: "packaging",
  UPLOADING: "uploading",
  COMPLETED: "completed",
  FAILED: "failed",
};

// Container <-> host path mapping (ffmate sees /workspace; host mounts it).
const WORKSPACE_CONTAINER = "/workspace";
const WORKSPACE_HOST = process.env.WORKSPACE_HOST || "/opt/media-workspace";
const toHost = (p) => p.replace(WORKSPACE_CONTAINER, WORKSPACE_HOST);

// Encoding knobs
const SEGMENT_SEC = Number(process.env.SEGMENT_SEC || 6);
const PRESET = process.env.X264_PRESET || "slow";
const LOUDNORM = process.env.LOUDNORM !== "0";
const LOUDNORM_I = process.env.LOUDNORM_I || "-16";

// Source probing
const PROBE_BIN = process.env.PROBE_BIN || "ffprobe";
const PROBE_CONTAINER = process.env.PROBE_CONTAINER === "1";
const SOURCE_HEIGHT_OVERRIDE = process.env.SOURCE_HEIGHT
  ? Number(process.env.SOURCE_HEIGHT)
  : null;

const LADDER = [
  { h: 2160, crf: 19, maxrate: "16000k", bufsize: "32000k", abr: "192k" },
  { h: 1440, crf: 19, maxrate: "11000k", bufsize: "22000k", abr: "192k" },
  { h: 1080, crf: 20, maxrate: "8000k", bufsize: "16000k", abr: "192k" },
  { h: 720, crf: 20, maxrate: "4500k", bufsize: "9000k", abr: "128k" },
  { h: 540, crf: 21, maxrate: "2500k", bufsize: "5000k", abr: "128k" },
  { h: 360, crf: 22, maxrate: "1200k", bufsize: "2400k", abr: "96k" },
  { h: 240, crf: 23, maxrate: "700k", bufsize: "1400k", abr: "64k" },
];

// R2
const R2 = {
  accountId: process.env.R2_ACCOUNT_ID,
  accessKeyId: process.env.R2_ACCESS_KEY_ID,
  secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  bucket: process.env.R2_BUCKET,
  publicBaseUrl: process.env.R2_PUBLIC_BASE_URL,
};
// Source masters live in the account's bucket (what the app uploaded to).
// Defaults to the same bucket; override if you keep sources separately.
const SOURCE_BUCKET = process.env.R2_SOURCE_BUCKET || R2.bucket;

const LOG_FILE = process.env.LOG_FILE || "/tmp/encoder.log";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(message, data = null) {
  const line =
    `[${new Date().toISOString()}] ${message}` +
    (data ? ` ${JSON.stringify(data)}` : "");
  console.log(line);
  try {
    fs.appendFileSync(LOG_FILE, line + "\n");
  } catch {
    /* non-fatal */
  }
}

// ---------------------------------------------------------------------------
// HTTP with timeout + retry (transient-only)
// ---------------------------------------------------------------------------
async function httpJson(url, opts = {}, timeoutMs = 30000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      const err = new Error(`HTTP ${res.status}: ${text}`);
      err.status = res.status;
      throw err;
    }
    if (res.status === 204) return null;
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

function transient(err) {
  if (err.status == null) return true;
  return err.status >= 500 || err.status === 429;
}

async function withRetry(fn, label, tries = 4) {
  let last;
  for (let i = 1; i <= tries; i++) {
    try {
      return await fn();
    } catch (e) {
      last = e;
      if (!transient(e) || i === tries) throw e;
      const backoff = 1000 * i;
      log(`Retry ${label} (${i}/${tries - 1})`, { error: e.message, backoff });
      await sleep(backoff);
    }
  }
  throw last;
}

// ---------------------------------------------------------------------------
// Directus layer — every pipeline step is recorded here
// ---------------------------------------------------------------------------
async function directusFetch(pathname, opts = {}) {
  const url = `${DIRECTUS_URL}/${pathname.replace(/^\//, "")}`;
  return withRetry(
    async () => {
      const res = await fetch(url, {
        ...opts,
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${DIRECTUS_TOKEN}`,
          ...(opts.headers || {}),
        },
      });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        const err = new Error(`Directus ${res.status}: ${text}`);
        err.status = res.status;
        throw err;
      }
      if (res.status === 204) return null;
      const j = await res.json();
      return j.data ?? j;
    },
    `directus ${opts.method || "GET"} ${pathname}`,
  );
}

/**
 * Near-realtime claim: grab the oldest queued job (FIFO), then atomically flip
 * it to "downloading" guarded by status=queued. If another worker won the race
 * the filtered update matches zero rows and we return null and poll again.
 * This is safe for multiple encoder nodes against one Directus.
 */
async function claimNextJob() {
  const list = await directusFetch(
    `items/media_jobs?filter[status][_eq]=${STATUS.QUEUED}` +
      `&sort=created_at&limit=1&fields=id,content_id,source_key,status`,
  );
  const job = Array.isArray(list) ? list[0] : null;
  if (!job) return null;

  const claimed = await directusFetch(`items/media_jobs`, {
    method: "PATCH",
    body: JSON.stringify({
      query: {
        filter: { id: { _eq: job.id }, status: { _eq: STATUS.QUEUED } },
      },
      data: {
        status: STATUS.DOWNLOADING,
        started_at: new Date().toISOString(),
        encoder_node: ENCODER_NODE,
      },
    }),
  });

  if (!Array.isArray(claimed) || claimed.length === 0) return null; // lost the race
  log("claimed job", {
    jobId: job.id,
    contentId: job.content_id,
    node: ENCODER_NODE,
  });
  return { ...job, status: STATUS.DOWNLOADING };
}

async function setJobStatus(jobId, status, extra = {}) {
  await directusFetch(`items/media_jobs/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ status, ...extra }),
  });
  log(`job → ${status}`, { jobId, ...extra });
}

async function completeJob(jobId) {
  await setJobStatus(jobId, STATUS.COMPLETED, {
    completed_at: new Date().toISOString(),
    error: null,
  });
}

async function failJob(jobId, message) {
  try {
    await setJobStatus(jobId, STATUS.FAILED, {
      error: String(message).slice(0, 1000),
      completed_at: new Date().toISOString(),
    });
  } catch (e) {
    log("could not mark job failed", { jobId, error: e.message });
  }
}

async function setContentHls(contentId, hlsUrl, durationSecs) {
  const data = { hls_url: hlsUrl };
  if (durationSecs && durationSecs > 0)
    data.duration_secs = Math.round(durationSecs);
  await directusFetch(`items/content_catalog/${contentId}`, {
    method: "PATCH",
    body: JSON.stringify(data),
  });
  log("content_catalog updated (playable)", { contentId, hlsUrl });
}

// ---------------------------------------------------------------------------
// ffmate task helpers
// ---------------------------------------------------------------------------
const getTaskId = (task) => task.uuid || task.id;

async function submitTask(payload) {
  const body = { ...payload };

  // ── ffmate webhook ─────────────────────────────────────────────────────
  // ffmate POSTs task state changes to these URLs. Shape per ffmate docs:
  //   "webhooks": [ { "event": "task.updated", "url": "https://..." } ]
  // The payload carries the task (uuid, status, progress) and its `metadata`,
  // so include content_id/job_id in metadata (below) to correlate the callback
  // back to the right media_job.
  if (FFMATE_WEBHOOK_URL) {
    body.webhooks = [{ event: FFMATE_WEBHOOK_EVENT, url: FFMATE_WEBHOOK_URL }];
  }

  return withRetry(
    () =>
      httpJson(`${BASE_URL}/api/v1/tasks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }),
    `submit ${payload.name}`,
  );
}

async function getTask(taskId) {
  return withRetry(
    () => httpJson(`${BASE_URL}/api/v1/tasks/${taskId}`),
    `get ${taskId}`,
  );
}

async function waitForTask(taskId, label, maxWaitMs = 6 * 60 * 60 * 1000) {
  log(`Waiting for ${label}`, { taskId });
  const start = Date.now();
  let lastStatus = null;

  while (true) {
    if (Date.now() - start > maxWaitMs) {
      throw new Error(
        `${label} timed out after ${Math.round(maxWaitMs / 1000)}s`,
      );
    }
    const task = await getTask(taskId);
    if (task.status !== lastStatus || task.status === "RUNNING") {
      log(`Status: ${label}`, { status: task.status, progress: task.progress });
      lastStatus = task.status;
    }
    if (task.status === "DONE_SUCCESSFUL") {
      log(`Completed: ${label}`);
      return task;
    }
    if (task.status === "DONE_ERROR" || task.status === "DONE_CANCELED") {
      const ffErr = (task.error || "").split("\n").slice(-6).join(" ").trim();
      log(`FAILED: ${label}`, { status: task.status, tail: ffErr });
      throw new Error(
        `${label} failed (${task.status}): ${ffErr || "see ffmate logs"}`,
      );
    }
    await sleep(3000);
  }
}

// ---------------------------------------------------------------------------
// Probe the source with ffprobe
// ---------------------------------------------------------------------------
function probeSource(ctx) {
  const target = PROBE_CONTAINER ? ctx.inputFile : toHost(ctx.inputFile);
  const [bin, ...prefixArgs] = PROBE_BIN.split(" ");
  const args = [
    ...prefixArgs,
    "-v",
    "error",
    "-print_format",
    "json",
    "-show_streams",
    "-show_format",
    target,
  ];

  let json;
  try {
    const out = execFileSync(bin, args, {
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
    });
    json = JSON.parse(out);
  } catch (e) {
    if (SOURCE_HEIGHT_OVERRIDE) {
      log("Probe failed; using SOURCE_HEIGHT override", {
        height: SOURCE_HEIGHT_OVERRIDE,
      });
      return {
        width: 0,
        height: SOURCE_HEIGHT_OVERRIDE,
        fps: 30,
        hasAudio: true,
        duration: 0,
      };
    }
    throw new Error(
      `ffprobe failed (${e.message}). Ensure ffprobe can read "${target}", ` +
        `or set PROBE_BIN / PROBE_CONTAINER / SOURCE_HEIGHT.`,
    );
  }

  const v = (json.streams || []).find((s) => s.codec_type === "video");
  const a = (json.streams || []).find((s) => s.codec_type === "audio");
  if (!v) throw new Error("Source has no video stream");

  const [num, den] = String(v.r_frame_rate || "0/1")
    .split("/")
    .map(Number);
  const fps = den ? num / den : 0;

  return {
    width: v.width || 0,
    height: v.height || 0,
    fps,
    hasAudio: !!a,
    duration: parseFloat((json.format && json.format.duration) || "0"),
  };
}

function buildLadder(srcHeight) {
  let cap = Math.min(srcHeight, 2160);
  cap = cap - (cap % 2);
  let rungs = LADDER.filter((r) => r.h <= cap);
  if (rungs.length === 0) rungs = [LADDER[LADDER.length - 1]];
  if (cap > rungs[0].h * 1.05) {
    const tier = LADDER.find((r) => r.h >= cap) || LADDER[0];
    rungs = [{ ...tier, h: cap }, ...rungs];
  }
  return rungs;
}

// ---------------------------------------------------------------------------
// Command builders  (encodeCommand uses ffmate ${INPUT_FILE}/${OUTPUT_FILE}
// substitution; hlsCommand builds explicit per-job paths)
// ---------------------------------------------------------------------------
function encodeCommand(rung, fps, hasAudio) {
  const gop = Math.max(2, Math.round((fps || 30) * SEGMENT_SEC));
  const parts = [
    "-y -i ${INPUT_FILE}",
    `-vf scale=-2:${rung.h}:flags=lanczos`,
    `-c:v libx264 -preset ${PRESET} -profile:v high -pix_fmt yuv420p`,
    `-crf ${rung.crf} -maxrate ${rung.maxrate} -bufsize ${rung.bufsize}`,
    `-x264-params "keyint=${gop}:min-keyint=${gop}:scenecut=0:open_gop=0"`,
    `-force_key_frames "expr:gte(t,n_forced*${SEGMENT_SEC})"`,
  ];
  if (hasAudio) {
    parts.push(`-c:a aac -ac 2 -ar 48000 -b:a ${rung.abr}`);
    if (LOUDNORM) parts.push(`-af loudnorm=I=${LOUDNORM_I}:TP=-1.5:LRA=11`);
  } else {
    parts.push("-an");
  }
  parts.push("-movflags +faststart");
  parts.push("${OUTPUT_FILE}");
  return parts.join(" ");
}

function hlsCommand(ctx, rungs, hasAudio) {
  const inputs = rungs.map((r) => `-i ${ctx.workdir}/${r.h}p.mp4`).join(" ");
  const maps = rungs
    .map((_, i) => (hasAudio ? `-map ${i}:v -map ${i}:a` : `-map ${i}:v`))
    .join(" ");
  const vsm = rungs
    .map((_, i) => (hasAudio ? `v:${i},a:${i}` : `v:${i}`))
    .join(" ");

  return [
    "-y",
    inputs,
    maps,
    "-c copy",
    "-f hls",
    `-hls_time ${SEGMENT_SEC}`,
    "-hls_playlist_type vod",
    "-hls_flags independent_segments",
    "-master_pl_name master.m3u8",
    `-var_stream_map "${vsm}"`,
    `-hls_segment_filename "${ctx.workdir}/hls/v%v/seg_%03d.ts"`,
    `"${ctx.workdir}/hls/v%v/index.m3u8"`,
  ].join(" ");
}

// ---------------------------------------------------------------------------
// R2
// ---------------------------------------------------------------------------
function makeR2Client() {
  const required = { ...R2 };
  delete required.publicBaseUrl; // not needed to construct the client
  const missing = Object.entries(required).filter(
    ([, v]) => !v || String(v).startsWith("<"),
  );
  if (missing.length) {
    throw new Error(`R2 config not set: ${missing.map(([k]) => k).join(", ")}`);
  }
  return new S3Client({
    region: "auto",
    endpoint: `https://${R2.accountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: R2.accessKeyId,
      secretAccessKey: R2.secretAccessKey,
    },
  });
}

function contentTypeFor(file) {
  if (file.endsWith(".m3u8")) return "application/vnd.apple.mpegurl";
  if (file.endsWith(".ts")) return "video/mp2t";
  if (file.endsWith(".m4s")) return "video/iso.segment";
  if (file.endsWith(".mp4")) return "video/mp4";
  return "application/octet-stream";
}

function listFilesRecursive(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...listFilesRecursive(full));
    else out.push(full);
  }
  return out;
}

async function downloadSourceFromR2(client, ctx) {
  const dest = toHost(ctx.inputFile);
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  log("Downloading source from R2", {
    bucket: SOURCE_BUCKET,
    key: ctx.sourceKey,
    dest,
  });

  const res = await withRetry(
    () =>
      client.send(
        new GetObjectCommand({ Bucket: SOURCE_BUCKET, Key: ctx.sourceKey }),
      ),
    `download ${ctx.sourceKey}`,
  );
  await pipeline(res.Body, fs.createWriteStream(dest));

  const size = fs.existsSync(dest) ? fs.statSync(dest).size : 0;
  if (!size) throw new Error(`Downloaded source is empty: ${dest}`);
  log("Source downloaded", { dest, bytes: size });
}

async function uploadHlsToR2(client, ctx) {
  if (!fs.existsSync(ctx.localHlsDir)) {
    throw new Error(`HLS dir not found on host: ${ctx.localHlsDir}`);
  }
  const files = listFilesRecursive(ctx.localHlsDir);
  if (!files.some((f) => f.endsWith("master.m3u8"))) {
    throw new Error("master.m3u8 not found — packaging may not have completed");
  }
  log("Uploading HLS to R2", { count: files.length, prefix: ctx.keyPrefix });

  const CONCURRENCY = 8;
  let i = 0;
  async function worker() {
    while (i < files.length) {
      const file = files[i++];
      const rel = path
        .relative(ctx.localHlsDir, file)
        .split(path.sep)
        .join("/");
      const key = `${ctx.keyPrefix}/${rel}`;
      await withRetry(
        () =>
          client.send(
            new PutObjectCommand({
              Bucket: R2.bucket,
              Key: key,
              Body: fs.createReadStream(file),
              ContentType: contentTypeFor(file),
              CacheControl: file.endsWith(".m3u8")
                ? "public, max-age=60"
                : "public, max-age=31536000, immutable",
            }),
          ),
        `upload ${key}`,
      );
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(CONCURRENCY, files.length) }, worker),
  );
  return `${R2.publicBaseUrl}/${ctx.keyPrefix}/master.m3u8`;
}

// ---------------------------------------------------------------------------
// Per-job pipeline
// ---------------------------------------------------------------------------
function makeCtx(job) {
  const jobId = job.id;
  const workdir = `${WORKSPACE_CONTAINER}/jobs/${jobId}`;
  const base = path.basename(job.source_key) || "source.mp4";
  return {
    jobId,
    contentId: job.content_id,
    sourceKey: job.source_key,
    workdir,
    inputFile: `${workdir}/source/${base}`,
    localHlsDir: toHost(`${workdir}/hls`),
    keyPrefix: `vod/${job.content_id}`, // stable playback path per content
  };
}

async function processJob(client, job) {
  const ctx = makeCtx(job);
  log("▶ processing", {
    jobId: ctx.jobId,
    contentId: ctx.contentId,
    source: ctx.sourceKey,
  });

  // 1. DOWNLOADING (status already set at claim) — pull source from R2.
  await downloadSourceFromR2(client, ctx);

  // 2. TRANSCODING — probe, build no-upscale ladder, encode all renditions.
  await setJobStatus(ctx.jobId, STATUS.TRANSCODING);
  const src = probeSource(ctx);
  log("Probed source", src);
  const rungs = buildLadder(src.height);
  log("Rendition ladder", {
    rungs: rungs.map((r) => `${r.h}p`),
    hasAudio: src.hasAudio,
  });

  const tasks = {};
  for (const rung of rungs) {
    const t = await submitTask({
      name: `encode_${rung.h}p`,
      command: encodeCommand(rung, src.fps, src.hasAudio),
      inputFile: ctx.inputFile,
      outputFile: `${ctx.workdir}/${rung.h}p.mp4`,
      priority: 10,
      metadata: {
        content_id: ctx.contentId,
        job_id: ctx.jobId,
        stage: "encode",
        rendition: `${rung.h}p`,
      },
    });
    tasks[`r${rung.h}`] = { id: getTaskId(t), rung };
  }
  await Promise.all(
    Object.values(tasks).map(({ id, rung }) => waitForTask(id, `${rung.h}p`)),
  );
  for (const rung of rungs) {
    const hostFile = toHost(`${ctx.workdir}/${rung.h}p.mp4`);
    if (!fs.existsSync(hostFile) || fs.statSync(hostFile).size === 0) {
      throw new Error(`Rendition missing or empty on host: ${hostFile}`);
    }
  }
  log("✅ All renditions completed");

  // 3. PACKAGING — variants done, HLS job starts.
  await setJobStatus(ctx.jobId, STATUS.PACKAGING);
  const dirs = rungs.map((_, i) => `${ctx.workdir}/hls/v${i}`).join(" ");
  const hls = await submitTask({
    name: "hls_packaging",
    command: hlsCommand(ctx, rungs, src.hasAudio),
    inputFile: `${ctx.workdir}/${rungs[0].h}p.mp4`,
    outputFile: `${ctx.workdir}/hls/master.m3u8`,
    priority: 10,
    preProcessing: { scriptPath: `mkdir -p ${dirs}` },
    metadata: {
      content_id: ctx.contentId,
      job_id: ctx.jobId,
      stage: "package",
    },
  });
  await waitForTask(getTaskId(hls), "HLS Packaging");
  log("🎬 HLS completed");

  // 4. UPLOADING — push HLS to R2.
  await setJobStatus(ctx.jobId, STATUS.UPLOADING);
  const hlsUrl = await uploadHlsToR2(client, ctx);
  log("☁️ Upload complete", { hlsUrl });

  // 5. Make it playable + mark the job done.
  await setContentHls(ctx.contentId, hlsUrl, src.duration);
  await completeJob(ctx.jobId);
  log(`✅ Job complete: ${hlsUrl}`);

  if (CLEANUP_AFTER) {
    try {
      fs.rmSync(toHost(ctx.workdir), { recursive: true, force: true });
      log("Cleaned workspace", { workdir: ctx.workdir });
    } catch (e) {
      log("Cleanup failed (non-fatal)", { error: e.message });
    }
  }
}

// ---------------------------------------------------------------------------
// Worker loop — poll + atomic claim, with idle backoff
// ---------------------------------------------------------------------------
async function workerLoop() {
  log("🚀 Encoder worker starting", {
    node: ENCODER_NODE,
    ffmate: BASE_URL,
    directus: DIRECTUS_URL,
    pollMs: POLL_INTERVAL_MS,
  });

  if (!DIRECTUS_URL || !DIRECTUS_TOKEN) {
    log("❌ DIRECTUS_URL and DIRECTUS_TOKEN (admin static token) are required");
    process.exit(1);
  }

  const client = makeR2Client();
  try {
    await client.send(new HeadBucketCommand({ Bucket: R2.bucket }));
    log("R2 bucket reachable", { bucket: R2.bucket });
  } catch (e) {
    log("⚠️ R2 bucket check failed (continuing)", { error: e.message });
  }

  let idleLogged = false;
  while (true) {
    let job = null;
    try {
      job = await claimNextJob();
    } catch (e) {
      log("Poll error (backing off)", { error: e.message });
      await sleep(POLL_INTERVAL_MS * 2);
      continue;
    }

    if (!job) {
      if (!idleLogged) {
        log("Idle — waiting for queued jobs");
        idleLogged = true;
      }
      await sleep(POLL_INTERVAL_MS);
      continue;
    }
    idleLogged = false;

    try {
      await processJob(client, job);
    } catch (e) {
      log("❌ Job failed", { jobId: job.id, error: e.message });
      await failJob(job.id, e.message);
    }
  }
}

workerLoop();
