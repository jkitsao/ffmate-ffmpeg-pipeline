import os from "os";
import path from "path";
import "dotenv/config";

// ============================================================================
// ENVIRONMENT & API CONFIG
// ============================================================================

export const AppConfig = {
  FFMATE_URL: process.env.FFMATE_URL,
  FFMATE_WEBHOOK_URL: process.env.FFMATE_WEBHOOK_URL || "",
  FFMATE_WEBHOOK_EVENT: process.env.FFMATE_WEBHOOK_EVENT || "task.updated",
  ENCODER_NODE: process.env.ENCODER_NODE || os.hostname(),
  POLL_INTERVAL_MS: Number(process.env.POLL_INTERVAL_MS || 3000),
  FFMATE_POLL_MS: Number(process.env.FFMATE_POLL_MS || 5000),
  ENCODE_CONCURRENCY: Number(process.env.ENCODE_CONCURRENCY || 2),
  CLEANUP_AFTER: process.env.CLEANUP_AFTER === "1",
  LOG_FILE: process.env.LOG_FILE || "/tmp/encoder.log",
};

export const DirectusConfig = {
  URL: (process.env.DIRECTUS_URL || "").replace(/\/+$/, ""),
  TOKEN: process.env.DIRECTUS_TOKEN || process.env.DIRECTUS_SERVER_TOKEN || "",
  STATUS: {
    QUEUED: "queued",
    DOWNLOADING: "downloading",
    TRANSCODING: "transcoding",
    PACKAGING: "packaging",
    UPLOADING: "uploading",
    COMPLETED: "completed",
    FAILED: "failed",
  },
};

export const R2Config = {
  accountId: process.env.R2_ACCOUNT_ID,
  accessKeyId: process.env.R2_ACCESS_KEY_ID,
  secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  bucket: process.env.R2_BUCKET,
  publicBaseUrl: process.env.R2_PUBLIC_BASE_URL,
  sourceBucket: process.env.R2_SOURCE_BUCKET || process.env.R2_BUCKET,
};

// ============================================================================
// ENCODING PROFILES & LADDERS
// ============================================================================

export const EncoderConfig = {
  WORKSPACE_CONTAINER: "/workspace",
  WORKSPACE_HOST: process.env.WORKSPACE_HOST || "/opt/media-workspace",
  PROBE_BIN: process.env.PROBE_BIN || "ffprobe",
  PROBE_CONTAINER: process.env.PROBE_CONTAINER === "1",
  SOURCE_HEIGHT_OVERRIDE: process.env.SOURCE_HEIGHT
    ? Number(process.env.SOURCE_HEIGHT)
    : null,

  SEGMENT_SEC: Number(process.env.SEGMENT_SEC || 6),
  // Options: "amf" (AMD Hardware) or "x264" (CPU)
  ENCODER_ENGINE: process.env.ENCODER_ENGINE || "x264",
  X264_PRESET: process.env.X264_PRESET || "slow",

  LOUDNORM: process.env.LOUDNORM !== "0",
  LOUDNORM_I: process.env.LOUDNORM_I || "-16",

  LADDER: [
    { h: 2160, crf: 19, maxrate: "16000k", bufsize: "32000k", abr: "192k" },
    { h: 1440, crf: 19, maxrate: "11000k", bufsize: "22000k", abr: "192k" },
    { h: 1080, crf: 20, maxrate: "8000k", bufsize: "16000k", abr: "192k" },
    { h: 720, crf: 20, maxrate: "4500k", bufsize: "9000k", abr: "128k" },
    { h: 540, crf: 21, maxrate: "2500k", bufsize: "5000k", abr: "128k" },
    { h: 360, crf: 22, maxrate: "1200k", bufsize: "2400k", abr: "96k" },
    { h: 240, crf: 23, maxrate: "700k", bufsize: "1400k", abr: "64k" },
  ],
};

// ============================================================================
// COMMAND BUILDERS
// ============================================================================

export const Commands = {
  // Translates a container workspace path to a host workspace path
  toHostPath: (p) =>
    p.replace(EncoderConfig.WORKSPACE_CONTAINER, EncoderConfig.WORKSPACE_HOST),

  /**
   * Dynamically build the encode ladder based on the source height
   */
  buildLadder: (srcHeight) => {
    let cap = Math.min(srcHeight, 2160);
    cap = cap - (cap % 2);
    let rungs = EncoderConfig.LADDER.filter((r) => r.h <= cap);
    if (rungs.length === 0)
      rungs = [EncoderConfig.LADDER[EncoderConfig.LADDER.length - 1]];

    // If the source is slightly larger than a standard rung, force a new top rung
    if (cap > rungs[0].h * 1.05) {
      const tier =
        EncoderConfig.LADDER.find((r) => r.h >= cap) || EncoderConfig.LADDER[0];
      rungs = [{ ...tier, h: cap }, ...rungs];
    }
    return rungs;
  },

  /**
   * Generates the transcode command for a single rung
   */
  encodeCommand: (rung, fps, hasAudio) => {
    const gop = Math.max(
      2,
      Math.round((fps || 30) * EncoderConfig.SEGMENT_SEC),
    );
    const parts = [
      "-y -i ${INPUT_FILE}",
      `-vf scale=-2:${rung.h}:flags=lanczos`,
    ];

    // Branch based on configured encoder engine
    if (EncoderConfig.ENCODER_ENGINE === "amf") {
      // AMD AMF Recommended Settings (per GPUOpen Wiki)
      // Uses vbr_peak, sets target bitrate based on maxrate for VOD quality
      const targetBitrate = Math.floor(parseInt(rung.maxrate) * 0.8) + "k"; // Target 80% of max
      parts.push(
        `-c:v h264_amf`,
        `-quality quality`, // Usage preset
        `-profile:v high`,
        `-rc vbr_peak`, // Peak VBR (recommended over CBR for VOD)
        `-b:v ${targetBitrate} -maxrate ${rung.maxrate}`,
      );
    } else {
      // Standard CPU x264 Settings
      parts.push(
        `-c:v libx264 -preset ${EncoderConfig.X264_PRESET} -profile:v high -pix_fmt yuv420p`,
        `-crf ${rung.crf} -maxrate ${rung.maxrate} -bufsize ${rung.bufsize}`,
        `-x264-params "keyint=${gop}:min-keyint=${gop}:scenecut=0:open_gop=0"`,
      );
    }

    // Strict keyframe alignment (critical for HLS packaging downstream)
    parts.push(
      `-force_key_frames "expr:gte(t,n_forced*${EncoderConfig.SEGMENT_SEC})"`,
    );

    // Audio Pipeline
    if (hasAudio) {
      parts.push(`-c:a aac -ac 2 -ar 48000 -b:a ${rung.abr}`);
      if (EncoderConfig.LOUDNORM) {
        parts.push(`-af loudnorm=I=${EncoderConfig.LOUDNORM_I}:TP=-1.5:LRA=11`);
      }
    } else {
      parts.push("-an");
    }

    parts.push("-movflags +faststart", "${OUTPUT_FILE}");
    return parts.join(" ");
  },

  /**
   * Generates the HLS packaging command (multiplexing all rungs)
   */
  hlsCommand: (workdir, rungs, hasAudio) => {
    const inputs = rungs.map((r) => `-i ${workdir}/${r.h}p.mp4`).join(" ");
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
      `-hls_time ${EncoderConfig.SEGMENT_SEC}`,
      "-hls_playlist_type vod",
      "-hls_flags independent_segments",
      "-master_pl_name master.m3u8",
      `-var_stream_map "${vsm}"`,
      // Fixes: "Conversion failed" at the end of stream copy
      "-max_muxing_queue_size 1024",
      // Fixes: Bitstream Annex-B errors by using fMP4 segments instead of old .ts files
      "-hls_segment_type fmp4",
      `-hls_segment_filename "${workdir}/hls/v%v/seg_%03d.m4s"`,
      `"${workdir}/hls/v%v/index.m3u8"`,
    ].join(" ");
  },
};
