import fs from "fs";
import path from "path";
import { execFileSync } from "child_process";
import {
  S3Client,
  PutObjectCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3"; // npm install @aws-sdk/client-s3

/* ============================================================================
 * OTT TRANSCODING PIPELINE  (ffmate + Cloudflare R2)
 *
 *   source  ->  probe  ->  per-rendition encode (normalized)  ->  HLS package
 *           ->  upload to R2  ->  return master.m3u8 URL
 *
 * Design goals: stability and broadcast-grade A/V quality.
 *  - PROBE the source; never assume resolution / fps / channels / audio.
 *  - NORMALIZE at encode time: 8-bit yuv420p, stereo 48 kHz AAC, loudness
 *    normalized, even dimensions, keyframes forced on segment boundaries.
 *  - NEVER UPSCALE: the ladder is capped at the source height.
 *  - PACKAGE with stream-copy (-c copy): fast, lossless, and impossible to
 *    break on an audio codec because nothing is re-encoded.
 *  - Keyframe alignment across renditions => clean ABR switching + clean cuts.
 * ==========================================================================*/

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------
const BASE_URL = process.env.FFMATE_URL;
const JOB_ID = process.env.JOB_ID || "video-001";

// Container <-> host path mapping.
// ffmate runs ffmpeg INSIDE its container and sees /workspace/...
// This script runs on the HOST, which mounts /opt/media-workspace -> /workspace,
// so it reads/probes files via the host path. toHost() converts between them.
const WORKSPACE_CONTAINER = "/workspace";
const WORKSPACE_HOST = process.env.WORKSPACE_HOST || "/opt/media-workspace";
const toHost = (p) => p.replace(WORKSPACE_CONTAINER, WORKSPACE_HOST);

const WORKDIR = `${WORKSPACE_CONTAINER}/jobs/${JOB_ID}`; // container path for outputs
const INPUT_FILE =
  process.env.INPUT_FILE ||
  `${WORKSPACE_CONTAINER}/test/1482055-hd_1920_1080_25fps.mp4`; // container path

// Encoding knobs ------------------------------------------------------------
const SEGMENT_SEC = Number(process.env.SEGMENT_SEC || 6); // HLS segment length
const PRESET = process.env.X264_PRESET || "slow"; // slow = superb quality/efficiency
const LOUDNORM = process.env.LOUDNORM !== "0"; // EBU R128 loudness normalize (on by default)
const LOUDNORM_I = process.env.LOUDNORM_I || "-16"; // streaming target LUFS (Apple/web = -16)

// Source probing ------------------------------------------------------------
//  PROBE_BIN: ffprobe invocation. Default assumes ffprobe on the host PATH.
//  If ffprobe only exists in the container, set e.g.:
//     PROBE_BIN="docker exec ffmate ffprobe"  and  PROBE_CONTAINER=1
const PROBE_BIN = process.env.PROBE_BIN || "ffprobe";
const PROBE_CONTAINER = process.env.PROBE_CONTAINER === "1"; // probe the container path instead of host path
const SOURCE_HEIGHT_OVERRIDE = process.env.SOURCE_HEIGHT
  ? Number(process.env.SOURCE_HEIGHT)
  : null;

// Rendition ladder (descending). Capped-CRF: CRF drives perceptual quality,
// maxrate/bufsize cap the bandwidth tier for ABR. Lower CRF = higher quality.
// 2160p/1440p are H.264 here for compatibility; consider HEVC/CMAF for 4K.
const LADDER = [
  { h: 2160, crf: 19, maxrate: "16000k", bufsize: "32000k", abr: "192k" },
  { h: 1440, crf: 19, maxrate: "11000k", bufsize: "22000k", abr: "192k" },
  { h: 1080, crf: 20, maxrate: "8000k", bufsize: "16000k", abr: "192k" },
  { h: 720, crf: 20, maxrate: "4500k", bufsize: "9000k", abr: "128k" },
  { h: 540, crf: 21, maxrate: "2500k", bufsize: "5000k", abr: "128k" },
  { h: 360, crf: 22, maxrate: "1200k", bufsize: "2400k", abr: "96k" },
  { h: 240, crf: 23, maxrate: "700k", bufsize: "1400k", abr: "64k" },
];

// ============================================================
//  R2 CONFIG  ——  PUT YOUR CLOUDFLARE R2 CREDENTIALS HERE
//  (env vars take precedence; placeholders are the fallback)
// ============================================================
const R2 = {
  accountId: process.env.R2_ACCOUNT_ID,
  accessKeyId: process.env.R2_ACCESS_KEY_ID,
  secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  bucket: process.env.R2_BUCKET,
  publicBaseUrl: process.env.R2_PUBLIC_BASE_URL,
};
const R2_KEY_PREFIX = process.env.R2_KEY_PREFIX || `vod/${JOB_ID}`;
const LOCAL_HLS_DIR = toHost(`${WORKDIR}/hls`); // host-side path the script reads to upload

// Logging -------------------------------------------------------------------
const LOG_FILE = `/tmp/${JOB_ID}.log`;
const REPORT_FILE = `/tmp/${JOB_ID}.report.json`;

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
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

function transient(err) {
  // retry on network errors, 5xx, and 429; never on other 4xx (deterministic)
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
// ffmate task helpers
// ---------------------------------------------------------------------------
const getTaskId = (task) => task.uuid || task.id;

async function submitTask(payload) {
  return withRetry(
    () =>
      httpJson(`${BASE_URL}/api/v1/tasks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
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
function probeSource() {
  const target = PROBE_CONTAINER ? INPUT_FILE : toHost(INPUT_FILE);
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

// Build the rendition ladder from the probed source height. Never upscales.
function buildLadder(srcHeight) {
  let cap = Math.min(srcHeight, 2160);
  cap = cap - (cap % 2); // even height

  let rungs = LADDER.filter((r) => r.h <= cap);
  if (rungs.length === 0) rungs = [LADDER[LADDER.length - 1]]; // tiny source -> smallest rung

  // If the source is meaningfully taller than our largest standard rung,
  // add a native-resolution top rung so full source detail is preserved.
  if (cap > rungs[0].h * 1.05) {
    const tier = LADDER.find((r) => r.h >= cap) || LADDER[0];
    rungs = [{ ...tier, h: cap }, ...rungs];
  }
  return rungs;
}

// ---------------------------------------------------------------------------
// Command builders
// ---------------------------------------------------------------------------
function encodeCommand(rung, fps, hasAudio) {
  const gop = Math.max(2, Math.round((fps || 30) * SEGMENT_SEC));

  const parts = [
    "-y -i ${INPUT_FILE}", // literal: ffmate substitutes
    `-vf scale=-2:${rung.h}:flags=lanczos`, // even width, no upscale (ladder-capped)
    `-c:v libx264 -preset ${PRESET} -profile:v high -pix_fmt yuv420p`,
    `-crf ${rung.crf} -maxrate ${rung.maxrate} -bufsize ${rung.bufsize}`,
    `-x264-params "keyint=${gop}:min-keyint=${gop}:scenecut=0:open_gop=0"`,
    `-force_key_frames "expr:gte(t,n_forced*${SEGMENT_SEC})"`, // aligned keyframes => copy-segmentable
  ];

  if (hasAudio) {
    parts.push(`-c:a aac -ac 2 -ar 48000 -b:a ${rung.abr}`); // always stereo 48k AAC
    if (LOUDNORM) parts.push(`-af loudnorm=I=${LOUDNORM_I}:TP=-1.5:LRA=11`);
  } else {
    parts.push("-an");
  }

  parts.push("-movflags +faststart");
  parts.push("${OUTPUT_FILE}"); // literal: ffmate substitutes
  return parts.join(" ");
}

function hlsCommand(rungs, hasAudio) {
  const inputs = rungs.map((r) => `-i ${WORKDIR}/${r.h}p.mp4`).join(" ");
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
    "-c copy", // lossless: renditions are already normalized + keyframe-aligned
    "-f hls",
    `-hls_time ${SEGMENT_SEC}`,
    "-hls_playlist_type vod",
    "-hls_flags independent_segments",
    "-master_pl_name master.m3u8",
    `-var_stream_map "${vsm}"`,
    `-hls_segment_filename "${WORKDIR}/hls/v%v/seg_%03d.ts"`,
    `"${WORKDIR}/hls/v%v/index.m3u8"`,
  ].join(" ");
}

// ---------------------------------------------------------------------------
// R2 upload
// ---------------------------------------------------------------------------
function makeR2Client() {
  const missing = Object.entries(R2).filter(
    ([, v]) => !v || String(v).startsWith("<"),
  );
  if (missing.length) {
    throw new Error(`R2 config not set: ${missing.map(([k]) => k).join(", ")}`);
  }
  return new S3Client({
    region: "auto", // R2 ignores region but the SDK requires a value
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

async function uploadHlsToR2(client) {
  if (!fs.existsSync(LOCAL_HLS_DIR)) {
    throw new Error(`HLS dir not found on host: ${LOCAL_HLS_DIR}`);
  }
  const files = listFilesRecursive(LOCAL_HLS_DIR);
  if (!files.some((f) => f.endsWith("master.m3u8"))) {
    throw new Error(
      "master.m3u8 not found in HLS output — packaging may not have completed",
    );
  }
  log("Uploading HLS to R2", { count: files.length, prefix: R2_KEY_PREFIX });

  const CONCURRENCY = 8;
  let i = 0;
  async function worker() {
    while (i < files.length) {
      const file = files[i++];
      const rel = path.relative(LOCAL_HLS_DIR, file).split(path.sep).join("/");
      const key = `${R2_KEY_PREFIX}/${rel}`;
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
      log("Uploaded", { key });
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(CONCURRENCY, files.length) }, worker),
  );

  return `${R2.publicBaseUrl}/${R2_KEY_PREFIX}/master.m3u8`;
}

// ---------------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------------
async function main() {
  log("🚀 Starting OTT pipeline", { job: JOB_ID, input: INPUT_FILE });
  const tasks = {};

  try {
    // 0. Validate R2 config up front so we never encode for an hour then fail to publish.
    const r2 = makeR2Client();
    try {
      await r2.send(new HeadBucketCommand({ Bucket: R2.bucket }));
      log("R2 bucket reachable", { bucket: R2.bucket });
    } catch (e) {
      log("⚠️ R2 bucket check failed (continuing; will retry at upload)", {
        error: e.message,
      });
    }

    // 1. Probe source and build a no-upscale ladder.
    const src = probeSource();
    log("Probed source", src);
    const rungs = buildLadder(src.height);
    log("Rendition ladder", {
      rungs: rungs.map((r) => `${r.h}p`),
      hasAudio: src.hasAudio,
    });

    // 2. Submit all encodes.
    log("Submitting encode tasks...");
    for (const rung of rungs) {
      const t = await submitTask({
        name: `encode_${rung.h}p`,
        command: encodeCommand(rung, src.fps, src.hasAudio),
        inputFile: INPUT_FILE,
        outputFile: `${WORKDIR}/${rung.h}p.mp4`,
        priority: 10,
      });
      tasks[`r${rung.h}`] = { id: getTaskId(t), rung };
    }

    // 3. Wait for all encodes in parallel — fail fast on the first error.
    await Promise.all(
      Object.values(tasks).map(({ id, rung }) => waitForTask(id, `${rung.h}p`)),
    );
    log("✅ All renditions completed");

    // 4. Verify outputs actually exist on disk (DONE_SUCCESSFUL but empty = catch it here).
    for (const rung of rungs) {
      const hostFile = toHost(`${WORKDIR}/${rung.h}p.mp4`);
      if (!fs.existsSync(hostFile) || fs.statSync(hostFile).size === 0) {
        throw new Error(`Rendition missing or empty on host: ${hostFile}`);
      }
    }

    // 5. HLS packaging (stream-copy; creates its own v0..vN dirs).
    log("Starting HLS packaging...");
    const dirs = rungs.map((_, i) => `${WORKDIR}/hls/v${i}`).join(" ");
    const hls = await submitTask({
      name: "hls_packaging",
      command: hlsCommand(rungs, src.hasAudio),
      inputFile: `${WORKDIR}/${rungs[0].h}p.mp4`,
      outputFile: `${WORKDIR}/hls/master.m3u8`,
      priority: 10,
      preProcessing: { scriptPath: `mkdir -p ${dirs}` },
    });
    await waitForTask(getTaskId(hls), "HLS Packaging");
    log("🎬 HLS completed successfully");

    // 6. Upload to R2 and return the playback URL.
    log("Uploading HLS output to Cloudflare R2...");
    const hlsUrl = await uploadHlsToR2(r2);
    log("☁️ Upload complete", { hlsUrl });

    // 7. Report.
    const report = {
      jobId: JOB_ID,
      status: "SUCCESS",
      source: src,
      ladder: rungs.map((r) => `${r.h}p`),
      hlsUrl,
      completedAt: new Date().toISOString(),
    };
    fs.writeFileSync(REPORT_FILE, JSON.stringify(report, null, 2));
    log("📦 Report written", { report: REPORT_FILE });
    log(`✅ Playback URL: ${hlsUrl}`);
    console.log(`\nHLS master playlist:\n${hlsUrl}\n`);
  } catch (err) {
    log("❌ Pipeline failed", { error: err.message });
    fs.writeFileSync(
      REPORT_FILE,
      JSON.stringify(
        { jobId: JOB_ID, status: "FAILED", error: err.message, tasks },
        null,
        2,
      ),
    );
    process.exit(1);
  }
}

main();
