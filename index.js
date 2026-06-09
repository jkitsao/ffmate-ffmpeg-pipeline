import fs from "fs";
import path from "path";
import { execFileSync } from "child_process";
import { pipeline } from "node:stream/promises";
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";
import { Config, STATUS, toHost, Commands } from "./config.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(message, data = null) {
  const line =
    `[${new Date().toISOString()}] ${message}` +
    (data ? ` ${JSON.stringify(data)}` : "");
  console.log(line);
  try {
    fs.appendFileSync(Config.LOG_FILE, line + "\n");
  } catch {
    /* non-fatal */
  }
}

// ============================================================================
// HTTP UTILS WITH TRANSIENT RETRIES
// ============================================================================
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
  const msg = String(err?.message || "");
  if (/database is locked|SQLITE_BUSY|001\.000\.0003/i.test(msg)) return true;
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

// ============================================================================
// DIRECTUS DB ORCHESTRATION INTERFACE
// ============================================================================
async function directusFetch(pathname, opts = {}) {
  const url = `${Config.DIRECTUS_URL}/${pathname.replace(/^\//, "")}`;
  return withRetry(
    async () => {
      const res = await fetch(url, {
        ...opts,
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${Config.DIRECTUS_TOKEN}`,
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
        encoder_node: Config.ENCODER_NODE,
      },
    }),
  });

  if (!Array.isArray(claimed) || claimed.length === 0) return null;
  log("claimed job", {
    jobId: job.id,
    contentId: job.content_id,
    node: Config.ENCODER_NODE,
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

// ============================================================================
// FFMATE RUNTIME TRANSLATOR
// ============================================================================
const getTaskId = (task) => task.uuid || task.id;

async function submitTask(payload) {
  const body = { ...payload };
  if (Config.FFMATE_WEBHOOK_URL) {
    body.webhooks = [
      { event: Config.FFMATE_WEBHOOK_EVENT, url: Config.FFMATE_WEBHOOK_URL },
    ];
  }
  return withRetry(
    () =>
      httpJson(`${Config.FFMATE_URL}/api/v1/tasks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }),
    `submit ${payload.name}`,
  );
}

async function waitForTask(taskId, label, maxWaitMs = 6 * 60 * 60 * 1000) {
  log(`Waiting for ${label}`, { taskId });
  const start = Date.now();
  let lastStatus = null;
  let pollErrors = 0;
  const MAX_POLL_ERRORS = 40;
  const jitter = () => Config.FFMATE_POLL_MS + Math.floor(Math.random() * 1000);

  while (true) {
    if (Date.now() - start > maxWaitMs) {
      throw new Error(
        `${label} timed out after ${Math.round(maxWaitMs / 1000)}s`,
      );
    }

    let task;
    try {
      task = await withRetry(
        () => httpJson(`${Config.FFMATE_URL}/api/v1/tasks/${taskId}`),
        `get ${taskId}`,
      );
      pollErrors = 0;
    } catch (e) {
      pollErrors++;
      log(`Poll hiccup for ${label} (ignored)`, {
        error: e.message,
        count: pollErrors,
      });
      if (pollErrors >= MAX_POLL_ERRORS) {
        throw new Error(
          `${label}: lost contact with ffmate after ${pollErrors} polls`,
        );
      }
      await sleep(jitter());
      continue;
    }

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
    await sleep(jitter());
  }
}

// ============================================================================
// FFPROBE DISCOVERY ANALYSIS & R2 ENGINE
// ============================================================================
function probeSource(ctx) {
  const target = Config.PROBE_CONTAINER ? ctx.inputFile : toHost(ctx.inputFile);
  const [bin, ...prefixArgs] = Config.PROBE_BIN.split(" ");
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
    if (Config.SOURCE_HEIGHT_OVERRIDE) {
      log("Probe failed; using SOURCE_HEIGHT override", {
        height: Config.SOURCE_HEIGHT_OVERRIDE,
      });
      return {
        width: 0,
        height: Config.SOURCE_HEIGHT_OVERRIDE,
        fps: 30,
        hasAudio: true,
        duration: 0,
      };
    }
    throw new Error(
      `ffprobe failed (${e.message}). Ensure pathing accessibility.`,
    );
  }

  const v = (json.streams || []).find((s) => s.codec_type === "video");
  const a = (json.streams || []).find((s) => s.codec_type === "audio");
  if (!v) throw new Error("Source has no video stream");

  const [num, den] = String(v.r_frame_rate || "0/1")
    .split("/")
    .map(Number);
  return {
    width: v.width || 0,
    height: v.height || 0,
    fps: den ? num / den : 0,
    hasAudio: !!a,
    duration: parseFloat((json.format && json.format.duration) || "0"),
  };
}

function makeR2Client() {
  const required = { ...Config.R2 };
  delete required.publicBaseUrl;
  const missing = Object.entries(required).filter(
    ([, v]) => !v || String(v).startsWith("<"),
  );
  if (missing.length)
    throw new Error(
      `R2 config missing values: ${missing.map(([k]) => k).join(", ")}`,
    );

  return new S3Client({
    region: "auto",
    endpoint: `https://${Config.R2.accountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: Config.R2.accessKeyId,
      secretAccessKey: Config.R2.secretAccessKey,
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
    bucket: Config.SOURCE_BUCKET,
    key: ctx.sourceKey,
    dest,
  });

  const res = await withRetry(
    () =>
      client.send(
        new GetObjectCommand({
          Bucket: Config.SOURCE_BUCKET,
          Key: ctx.sourceKey,
        }),
      ),
    `download ${ctx.sourceKey}`,
  );
  await pipeline(res.Body, fs.createWriteStream(dest));

  const size = fs.existsSync(dest) ? fs.statSync(dest).size : 0;
  if (!size) throw new Error(`Downloaded source is empty: ${dest}`);
  log("Source downloaded", { dest, bytes: size });
}

async function uploadHlsToR2(client, ctx) {
  if (!fs.existsSync(ctx.localHlsDir))
    throw new Error(`HLS dir not found on host: ${ctx.localHlsDir}`);
  const files = listFilesRecursive(ctx.localHlsDir);
  if (!files.some((f) => f.endsWith("master.m3u8")))
    throw new Error("master.m3u8 missing — check execution contexts.");
  log("Uploading HLS to R2", { count: files.length, prefix: ctx.keyPrefix });

  const CONCURRENCY = 8;
  let idx = 0;
  async function worker() {
    while (idx < files.length) {
      const file = files[idx++];
      const rel = path
        .relative(ctx.localHlsDir, file)
        .split(path.sep)
        .join("/");
      const key = `${ctx.keyPrefix}/${rel}`;
      await withRetry(
        () =>
          client.send(
            new PutObjectCommand({
              Bucket: Config.R2.bucket,
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
  return `${Config.R2.publicBaseUrl}/${ctx.keyPrefix}/master.m3u8`;
}

// ============================================================================
// LOGIC PIPELINE CONTAINER IMPLEMENTATION
// ============================================================================
function makeCtx(job) {
  const workdir = `${Config.WORKSPACE_CONTAINER}/jobs/${job.id}`;
  const base = path.basename(job.source_key) || "source.mp4";
  return {
    jobId: job.id,
    contentId: job.content_id,
    sourceKey: job.source_key,
    workdir,
    inputFile: `${workdir}/source/${base}`,
    localHlsDir: toHost(`${workdir}/hls`),
    keyPrefix: `vod/${job.content_id}`,
  };
}

async function processJob(client, job) {
  const ctx = makeCtx(job);
  log("▶ processing", {
    jobId: ctx.jobId,
    contentId: ctx.contentId,
    source: ctx.sourceKey,
  });

  // 1. Download Master File
  await downloadSourceFromR2(client, ctx);

  // 2. Transcode Variants
  await setJobStatus(ctx.jobId, STATUS.TRANSCODING);
  const src = probeSource(ctx);
  log("Probed source", src);
  const rungs = Commands.buildLadder(src.height);
  log("Rendition ladder", {
    rungs: rungs.map((r) => `${r.h}p`),
    hasAudio: src.hasAudio,
  });

  for (let i = 0; i < rungs.length; i += Config.ENCODE_CONCURRENCY) {
    const batch = rungs.slice(i, i + Config.ENCODE_CONCURRENCY);
    const submitted = [];
    for (const rung of batch) {
      const t = await submitTask({
        name: `encode_${rung.h}p`,
        command: Commands.encodeCommand(rung, src.fps, src.hasAudio),
        inputFile: ctx.inputFile,
        outputFile: `${ctx.workdir}/${rung.h}p.mp4`,
        priority: 10,
        // METADATA RESTORED: Frontend status webhooks will properly sync progress tracking
        metadata: {
          content_id: ctx.contentId,
          job_id: ctx.jobId,
          stage: "encode",
          rendition: `${rung.h}p`,
        },
      });
      submitted.push({ id: getTaskId(t), rung });
    }
    await Promise.all(
      submitted.map(({ id, rung }) => waitForTask(id, `${rung.h}p`)),
    );
  }

  for (const rung of rungs) {
    const hostFile = toHost(`${ctx.workdir}/${rung.h}p.mp4`);
    if (!fs.existsSync(hostFile) || fs.statSync(hostFile).size === 0) {
      throw new Error(`Rendition missing or empty on host: ${hostFile}`);
    }
  }
  log("✅ All renditions completed");

  // 3. Package Stream Variants Into Multi-Bitrate fMP4 HLS Playlist
  await setJobStatus(ctx.jobId, STATUS.PACKAGING);
  // ffmpeg's HLS muxer does NOT create directories — make the (single, flat)
  // hls/ output dir ourselves. The missing dir was what failed packaging.
  fs.mkdirSync(ctx.localHlsDir, { recursive: true });
  const hls = await submitTask({
    name: "hls_packaging",
    command: Commands.hlsCommand(ctx, rungs, src.hasAudio),
    inputFile: `${ctx.workdir}/${rungs[0].h}p.mp4`,
    outputFile: `${ctx.workdir}/hls/master.m3u8`,
    priority: 10,
    // METADATA RESTORED: Syncs final packaging lifecycle alerts with frontend dashboard
    metadata: {
      content_id: ctx.contentId,
      job_id: ctx.jobId,
      stage: "package",
    },
  });
  await waitForTask(getTaskId(hls), "HLS Packaging");
  log("🎬 HLS completed");

  // 4. Transport to R2 Production Bucket
  await setJobStatus(ctx.jobId, STATUS.UPLOADING);
  const hlsUrl = await uploadHlsToR2(client, ctx);
  log("☁️ Upload complete", { hlsUrl });

  // 5. Callback Finalization Updates
  await setContentHls(ctx.contentId, hlsUrl, src.duration);
  await completeJob(ctx.jobId);
  log(`✅ Job complete: ${hlsUrl}`);

  if (Config.CLEANUP_AFTER) {
    try {
      fs.rmSync(toHost(ctx.workdir), { recursive: true, force: true });
      log("Cleaned workspace", { workdir: ctx.workdir });
    } catch (e) {
      log("Cleanup failed (non-fatal)", { error: e.message });
    }
  }
}

// ============================================================================
// CORE LOOP REPLICATOR
// ============================================================================
async function workerLoop() {
  log("🚀 Encoder worker starting", {
    node: Config.ENCODER_NODE,
    ffmate: Config.FFMATE_URL,
    directus: Config.DIRECTUS_URL,
    pollMs: Config.POLL_INTERVAL_MS,
  });

  if (!Config.DIRECTUS_URL || !Config.DIRECTUS_TOKEN) {
    log(
      "❌ DIRECTUS_URL and DIRECTUS_TOKEN are required configuration elements",
    );
    process.exit(1);
  }

  const client = makeR2Client();
  try {
    await client.send(new HeadBucketCommand({ Bucket: Config.R2.bucket }));
    log("R2 bucket reachable", { bucket: Config.R2.bucket });
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
      await sleep(Config.POLL_INTERVAL_MS * 2);
      continue;
    }

    if (!job) {
      if (!idleLogged) {
        log("Idle — waiting for queued jobs");
        idleLogged = true;
      }
      await sleep(Config.POLL_INTERVAL_MS);
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