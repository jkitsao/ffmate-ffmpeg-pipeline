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
import {
  AppConfig,
  DirectusConfig,
  R2Config,
  EncoderConfig,
  Commands,
} from "./config.js";

// ============================================================================
// UTILS & LOGGING
// ============================================================================

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(message, data = null) {
  const line =
    `[${new Date().toISOString()}] ${message}` +
    (data ? ` ${JSON.stringify(data)}` : "");
  console.log(line);
  try {
    fs.appendFileSync(AppConfig.LOG_FILE, line + "\n");
  } catch {
    /* non-fatal */
  }
}

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
// DIRECTUS API
// ============================================================================

async function directusFetch(pathname, opts = {}) {
  const url = `${DirectusConfig.URL}/${pathname.replace(/^\//, "")}`;
  return withRetry(
    async () => {
      const res = await fetch(url, {
        ...opts,
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${DirectusConfig.TOKEN}`,
          ...(opts.headers || {}),
        },
      });
      if (!res.ok) {
        const err = new Error(
          `Directus ${res.status}: ${await res.text().catch(() => "")}`,
        );
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
    `items/media_jobs?filter[status][_eq]=${DirectusConfig.STATUS.QUEUED}&sort=created_at&limit=1&fields=id,content_id,source_key,status`,
  );
  const job = Array.isArray(list) ? list[0] : null;
  if (!job) return null;

  const claimed = await directusFetch(`items/media_jobs`, {
    method: "PATCH",
    body: JSON.stringify({
      query: {
        filter: {
          id: { _eq: job.id },
          status: { _eq: DirectusConfig.STATUS.QUEUED },
        },
      },
      data: {
        status: DirectusConfig.STATUS.DOWNLOADING,
        started_at: new Date().toISOString(),
        encoder_node: AppConfig.ENCODER_NODE,
      },
    }),
  });

  if (!Array.isArray(claimed) || claimed.length === 0) return null;
  log("claimed job", { jobId: job.id, node: AppConfig.ENCODER_NODE });
  return { ...job, status: DirectusConfig.STATUS.DOWNLOADING };
}

async function setJobStatus(jobId, status, extra = {}) {
  await directusFetch(`items/media_jobs/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ status, ...extra }),
  });
  log(`job → ${status}`, { jobId, ...extra });
}

async function completeJob(jobId) {
  await setJobStatus(jobId, DirectusConfig.STATUS.COMPLETED, {
    completed_at: new Date().toISOString(),
    error: null,
  });
}

async function failJob(jobId, message) {
  try {
    await setJobStatus(jobId, DirectusConfig.STATUS.FAILED, {
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
}

// ============================================================================
// FFMATE TASK ORCHESTRATION
// ============================================================================

const getTaskId = (task) => task.uuid || task.id;

async function submitTask(payload) {
  const body = { ...payload };
  if (AppConfig.FFMATE_WEBHOOK_URL) {
    body.webhooks = [
      {
        event: AppConfig.FFMATE_WEBHOOK_EVENT,
        url: AppConfig.FFMATE_WEBHOOK_URL,
      },
    ];
  }

  return withRetry(
    () =>
      httpJson(`${AppConfig.FFMATE_URL}/api/v1/tasks`, {
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

  while (true) {
    if (Date.now() - start > maxWaitMs) throw new Error(`${label} timed out`);

    let task;
    try {
      task = await withRetry(
        () => httpJson(`${AppConfig.FFMATE_URL}/api/v1/tasks/${taskId}`),
        `get ${taskId}`,
      );
      pollErrors = 0;
    } catch (e) {
      pollErrors++;
      if (pollErrors >= 40)
        throw new Error(`${label}: lost contact with ffmate`);
      await sleep(AppConfig.FFMATE_POLL_MS);
      continue;
    }

    if (task.status !== lastStatus || task.status === "RUNNING") {
      log(`Status: ${label}`, { status: task.status, progress: task.progress });
      lastStatus = task.status;
    }
    if (task.status === "DONE_SUCCESSFUL") return task;
    if (task.status === "DONE_ERROR" || task.status === "DONE_CANCELED") {
      throw new Error(
        `${label} failed (${task.status}): ${task.error || "see ffmate logs"}`,
      );
    }
    await sleep(AppConfig.FFMATE_POLL_MS + Math.floor(Math.random() * 1000));
  }
}

// ============================================================================
// PROBE & R2
// ============================================================================

function probeSource(ctx) {
  const target = EncoderConfig.PROBE_CONTAINER
    ? ctx.inputFile
    : Commands.toHostPath(ctx.inputFile);
  const [bin, ...prefixArgs] = EncoderConfig.PROBE_BIN.split(" ");
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

  try {
    const out = execFileSync(bin, args, {
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
    });
    const json = JSON.parse(out);
    const v = (json.streams || []).find((s) => s.codec_type === "video");
    const a = (json.streams || []).find((s) => s.codec_type === "audio");
    if (!v) throw new Error("No video stream");

    const [num, den] = String(v.r_frame_rate || "0/1")
      .split("/")
      .map(Number);
    return {
      width: v.width || 0,
      height: v.height || 0,
      fps: den ? num / den : 0,
      hasAudio: !!a,
      duration: parseFloat(json.format?.duration || "0"),
    };
  } catch (e) {
    if (EncoderConfig.SOURCE_HEIGHT_OVERRIDE) {
      return {
        width: 0,
        height: EncoderConfig.SOURCE_HEIGHT_OVERRIDE,
        fps: 30,
        hasAudio: true,
        duration: 0,
      };
    }
    throw e;
  }
}

function makeR2Client() {
  return new S3Client({
    region: "auto",
    endpoint: `https://${R2Config.accountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: R2Config.accessKeyId,
      secretAccessKey: R2Config.secretAccessKey,
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

// ============================================================================
// WORKER PIPELINE
// ============================================================================

async function processJob(client, job) {
  const workdir = `${EncoderConfig.WORKSPACE_CONTAINER}/jobs/${job.id}`;
  const ctx = {
    jobId: job.id,
    contentId: job.content_id,
    sourceKey: job.source_key,
    workdir,
    inputFile: `${workdir}/source/${path.basename(job.source_key) || "source.mp4"}`,
    localHlsDir: Commands.toHostPath(`${workdir}/hls`),
    keyPrefix: `vod/${job.content_id}`,
  };

  log("▶ processing", { jobId: ctx.jobId });

  // 1. Download
  const dest = Commands.toHostPath(ctx.inputFile);
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  const res = await withRetry(
    () =>
      client.send(
        new GetObjectCommand({
          Bucket: R2Config.sourceBucket,
          Key: ctx.sourceKey,
        }),
      ),
    "download",
  );
  await pipeline(res.Body, fs.createWriteStream(dest));

  // 2. Transcode
  await setJobStatus(ctx.jobId, DirectusConfig.STATUS.TRANSCODING);
  const src = probeSource(ctx);
  const rungs = Commands.buildLadder(src.height);

  for (let i = 0; i < rungs.length; i += AppConfig.ENCODE_CONCURRENCY) {
    const batch = rungs.slice(i, i + AppConfig.ENCODE_CONCURRENCY);
    const submitted = await Promise.all(
      batch.map(async (rung) => {
        const t = await submitTask({
          name: `encode_${rung.h}p`,
          command: Commands.encodeCommand(rung, src.fps, src.hasAudio),
          inputFile: ctx.inputFile,
          outputFile: `${ctx.workdir}/${rung.h}p.mp4`,
          priority: 10,
        });
        return { id: getTaskId(t), rung };
      }),
    );
    await Promise.all(
      submitted.map(({ id, rung }) => waitForTask(id, `${rung.h}p`)),
    );
  }

  // 3. Package
  await setJobStatus(ctx.jobId, DirectusConfig.STATUS.PACKAGING);
  const dirs = rungs.map((_, i) => `${ctx.workdir}/hls/v${i}`).join(" ");
  const hls = await submitTask({
    name: "hls_packaging",
    command: Commands.hlsCommand(ctx.workdir, rungs, src.hasAudio),
    inputFile: `${ctx.workdir}/${rungs[0].h}p.mp4`,
    outputFile: `${ctx.workdir}/hls/master.m3u8`,
    preProcessing: { scriptPath: `mkdir -p ${dirs}` },
  });
  await waitForTask(getTaskId(hls), "HLS Packaging");

  // 4. Upload
  await setJobStatus(ctx.jobId, DirectusConfig.STATUS.UPLOADING);
  const files = listFilesRecursive(ctx.localHlsDir);
  await Promise.all(
    files.map((file) => {
      const key = `${ctx.keyPrefix}/${path.relative(ctx.localHlsDir, file).split(path.sep).join("/")}`;
      return withRetry(
        () =>
          client.send(
            new PutObjectCommand({
              Bucket: R2Config.bucket,
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
    }),
  );

  const hlsUrl = `${R2Config.publicBaseUrl}/${ctx.keyPrefix}/master.m3u8`;
  await setContentHls(ctx.contentId, hlsUrl, src.duration);
  await completeJob(ctx.jobId);
  log(`✅ Job complete: ${hlsUrl}`);

  if (AppConfig.CLEANUP_AFTER) {
    fs.rmSync(Commands.toHostPath(ctx.workdir), {
      recursive: true,
      force: true,
    });
  }
}

async function workerLoop() {
  log("🚀 Encoder worker starting", { node: AppConfig.ENCODER_NODE });
  const client = makeR2Client();

  while (true) {
    let job = await claimNextJob().catch(() => null);
    if (!job) {
      await sleep(AppConfig.POLL_INTERVAL_MS);
      continue;
    }
    try {
      await processJob(client, job);
    } catch (e) {
      log("❌ Job failed", { jobId: job.id, error: e.message });
      await failJob(job.id, e.message);
    }
  }
}

workerLoop();
