// config.js — all configuration + every ffmpeg command in one swappable place.
// index.js imports { Config, STATUS, toHost, Commands } and never builds a
// command itself. Swap encoders with ENCODER=x264|amf|nvenc; tune the ladder
// or add a profile here without touching the orchestration logic.

import os from "os";

// ============================================================================
// Config (env-driven)
// ============================================================================
const num = (v, d) => (v == null || v === "" ? d : Number(v));

export const Config = {
  // ffmate
  FFMATE_URL: process.env.FFMATE_URL,
  FFMATE_WEBHOOK_URL: process.env.FFMATE_WEBHOOK_URL || "",
  FFMATE_WEBHOOK_EVENT: process.env.FFMATE_WEBHOOK_EVENT || "task.updated",
  FFMATE_POLL_MS: num(process.env.FFMATE_POLL_MS, 5000),

  // Directus
  DIRECTUS_URL: (process.env.DIRECTUS_URL || "").replace(/\/+$/, ""),
  DIRECTUS_TOKEN:
    process.env.DIRECTUS_TOKEN || process.env.DIRECTUS_SERVER_TOKEN || "",

  // worker
  ENCODER_NODE: process.env.ENCODER_NODE || os.hostname(),
  POLL_INTERVAL_MS: num(process.env.POLL_INTERVAL_MS, 3000),
  ENCODE_CONCURRENCY: num(process.env.ENCODE_CONCURRENCY, 2),
  CLEANUP_AFTER: process.env.CLEANUP_AFTER === "1",
  LOG_FILE: process.env.LOG_FILE || "/tmp/encoder.log",

  // encoding selection + knobs (consumed by Commands below)
  ENCODER: (process.env.ENCODER || "x264").toLowerCase(), // x264 | amf | nvenc
  SEGMENT_SEC: num(process.env.SEGMENT_SEC, 6),
  X264_PRESET: process.env.X264_PRESET || "slow",
  LOUDNORM: process.env.LOUDNORM !== "0",
  LOUDNORM_I: process.env.LOUDNORM_I || "-16",

  // probing
  PROBE_BIN: process.env.PROBE_BIN || "ffprobe",
  PROBE_CONTAINER: process.env.PROBE_CONTAINER === "1",
  SOURCE_HEIGHT_OVERRIDE: process.env.SOURCE_HEIGHT
    ? Number(process.env.SOURCE_HEIGHT)
    : null,

  // workspace (container path that ffmate sees ↔ host mount)
  WORKSPACE_CONTAINER: process.env.WORKSPACE_CONTAINER || "/workspace",
  WORKSPACE_HOST: process.env.WORKSPACE_HOST || "/opt/media-workspace",

  // R2
  R2: {
    accountId: process.env.R2_ACCOUNT_ID,
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    bucket: process.env.R2_BUCKET,
    publicBaseUrl: process.env.R2_PUBLIC_BASE_URL,
  },
  SOURCE_BUCKET: process.env.R2_SOURCE_BUCKET || process.env.R2_BUCKET,
};

// media_jobs status vocabulary (must match the Directus field's allowed values)
export const STATUS = {
  QUEUED: "queued",
  DOWNLOADING: "downloading",
  TRANSCODING: "transcoding",
  PACKAGING: "packaging",
  UPLOADING: "uploading",
  COMPLETED: "completed",
  FAILED: "failed",
};

// container → host path mapping
export const toHost = (p) =>
  p.replace(Config.WORKSPACE_CONTAINER, Config.WORKSPACE_HOST);

// ============================================================================
// ABR ladder
// ============================================================================
// Rates in kbps. `crf` = quality target for CRF-capable encoders (x264);
// GPU profiles derive an average bitrate from maxK and cap with maxrate.
const LADDER = [
  { h: 2160, crf: 19, maxK: 16000, bufK: 32000, aK: 192 },
  { h: 1440, crf: 19, maxK: 11000, bufK: 22000, aK: 192 },
  { h: 1080, crf: 20, maxK: 8000,  bufK: 16000, aK: 192 },
  { h: 720,  crf: 20, maxK: 4500,  bufK: 9000,  aK: 128 },
  { h: 540,  crf: 21, maxK: 2500,  bufK: 5000,  aK: 128 },
  { h: 360,  crf: 22, maxK: 1200,  bufK: 2400,  aK: 96 },
  { h: 240,  crf: 23, maxK: 700,   bufK: 1400,  aK: 64 },
];

function buildLadder(srcHeight) {
  let cap = Math.min(srcHeight || 0, 2160);
  cap = cap - (cap % 2);
  let rungs = LADDER.filter((r) => r.h <= cap);
  if (rungs.length === 0) rungs = [LADDER[LADDER.length - 1]];
  if (cap > rungs[0].h * 1.05) {
    const tier = LADDER.find((r) => r.h >= cap) || LADDER[0];
    rungs = [{ ...tier, h: cap }, ...rungs];
  }
  // test helpers: LADDER_HEIGHTS=720,240 to encode only those; MAX_RUNGS=1 to cap
  const only = (process.env.LADDER_HEIGHTS || "")
    .split(",").map((s) => Number(s.trim())).filter(Boolean);
  if (only.length) rungs = rungs.filter((r) => only.includes(r.h));
  const maxR = Number(process.env.MAX_RUNGS || 0);
  if (maxR > 0) rungs = rungs.slice(0, maxR);
  if (rungs.length === 0) rungs = [LADDER[LADDER.length - 1]];
  return rungs;
}

// ============================================================================
// Encoder profiles — each returns ONLY the video-codec args for one rung.
// IMPORTANT: -rc / -cq / -preanalysis are encoder-PRIVATE options. They are
// valid only here, after -c:v <gpu encoder>. libx264 must NOT receive -rc.
// ============================================================================
const gopFor = (fps) => Math.max(2, Math.round((fps || 30) * Config.SEGMENT_SEC));
const kfExpr = () =>
  `-force_key_frames "expr:gte(t,n_forced*${Config.SEGMENT_SEC})"`;

const ENCODERS = {
  // CPU — most portable, best quality-per-bit. CRF + capped VBR. NO -rc.
  x264: (rung, fps) => {
    const gop = gopFor(fps);
    return [
      `-c:v libx264 -preset ${Config.X264_PRESET} -profile:v high -pix_fmt yuv420p`,
      `-crf ${rung.crf} -maxrate ${rung.maxK}k -bufsize ${rung.bufK}k`,
      `-x264-params "keyint=${gop}:min-keyint=${gop}:scenecut=0:open_gop=0"`,
      kfExpr(),
    ].join(" ");
  },

  // AMD GPU (AMF) — per AMD's recommended FFmpeg encoder settings:
  // https://github.com/GPUOpen-LibrariesAndSDKs/AMF/wiki/Recommended-FFmpeg-Encoder-Settings
  // transcoding usage + quality preset + capped VBR (vbr_peak) + pre-analysis,
  // fixed GOP so segments stay keyframe-aligned for stream-copy packaging.
  amf: (rung, fps) => {
    const gop = gopFor(fps);
    const target = Math.round(rung.maxK * 0.6); // average under the peak cap
    return [
      `-c:v h264_amf`,
      `-usage transcoding`,
      `-quality quality`,
      `-rc vbr_peak -b:v ${target}k -maxrate ${rung.maxK}k -bufsize ${rung.bufK}k`,
      `-preanalysis true`,
      `-profile:v high -level 4.2 -bf 3`,
      `-g ${gop}`,
      kfExpr(),
    ].join(" ");
  },

  // NVIDIA GPU (NVENC) — capped VBR with a quality target (cq ≈ crf).
  nvenc: (rung, fps) => {
    const gop = gopFor(fps);
    const target = Math.round(rung.maxK * 0.6);
    return [
      `-c:v h264_nvenc -preset p5 -tune hq`,
      `-rc vbr -cq ${rung.crf} -b:v ${target}k -maxrate ${rung.maxK}k -bufsize ${rung.bufK}k`,
      `-profile:v high -g ${gop} -forced-idr 1`,
      kfExpr(),
    ].join(" ");
  },
};

function audioArgs(hasAudio, rung) {
  if (!hasAudio) return "-an";
  let a = `-c:a aac -ac 2 -ar 48000 -b:a ${rung.aK}k`;
  if (Config.LOUDNORM)
    a += ` -af loudnorm=I=${Config.LOUDNORM_I}:TP=-1.5:LRA=11`;
  return a;
}

// ============================================================================
// Commands — the swappable command surface index.js calls
// ============================================================================
export const Commands = {
  buildLadder,

  // signature matches index.js: (rung, fps, hasAudio)
  encodeCommand(rung, fps, hasAudio) {
    const profile = ENCODERS[Config.ENCODER] || ENCODERS.x264;
    return [
      "-y -i ${INPUT_FILE}",
      `-vf scale=-2:${rung.h}:flags=lanczos`,
      profile(rung, fps),
      audioArgs(hasAudio, rung),
      "-movflags +faststart",
      "${OUTPUT_FILE}",
    ].join(" ");
  },

  // Stream-copy the per-rung MP4s into one multi-variant master. FLAT output
  // (v0.m3u8 / v0_000.ts) — NO %v subdirectories — so packaging never depends
  // on pre-created subdirs. index.js makes the single hls/ dir before running.
  hlsCommand(ctx, rungs, hasAudio) {
    const hls = `${ctx.workdir}/hls`;
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
      `-hls_time ${Config.SEGMENT_SEC}`,
      "-hls_playlist_type vod",
      "-hls_flags independent_segments",
      "-master_pl_name master.m3u8",
      `-var_stream_map "${vsm}"`,
      `-hls_segment_filename "${hls}/v%v_%03d.ts"`,
      `"${hls}/v%v.m3u8"`,
    ].join(" ");
  },
};