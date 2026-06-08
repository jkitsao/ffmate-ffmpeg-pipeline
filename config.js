import os from "os";
import "dotenv/config";

// ============================================================================
// SYSTEM & PIPELINE ENVIRONMENT VARIABLES
// ============================================================================
export const Config = {
  FFMATE_URL: process.env.FFMATE_URL,
  FFMATE_WEBHOOK_URL: process.env.FFMATE_WEBHOOK_URL || "",
  FFMATE_WEBHOOK_EVENT: process.env.FFMATE_WEBHOOK_EVENT || "task.updated",

  DIRECTUS_URL: (process.env.DIRECTUS_URL || "").replace(/\/+$/, ""),
  DIRECTUS_TOKEN:
    process.env.DIRECTUS_TOKEN || process.env.DIRECTUS_SERVER_TOKEN || "",

  ENCODER_NODE: process.env.ENCODER_NODE || os.hostname(),
  POLL_INTERVAL_MS: Number(process.env.POLL_INTERVAL_MS || 3000),
  FFMATE_POLL_MS: Number(process.env.FFMATE_POLL_MS || 5000),
  ENCODE_CONCURRENCY: Number(process.env.ENCODE_CONCURRENCY || 2),
  CLEANUP_AFTER: process.env.CLEANUP_AFTER === "1",
  LOG_FILE: process.env.LOG_FILE || "/tmp/encoder.log",

  WORKSPACE_CONTAINER: "/workspace",
  WORKSPACE_HOST: process.env.WORKSPACE_HOST || "/opt/media-workspace",

  SEGMENT_SEC: Number(process.env.SEGMENT_SEC || 6),

  // ── ENCODER HARDWARE SWITCH ───────────────────────────────────────────────
  // Change to "x264" for CPU encoding, or "amf" for AMD Hardware Acceleration
  ENCODER_ENGINE: process.env.ENCODER_ENGINE || "amf",
  X264_PRESET: process.env.X264_PRESET || "slow",

  LOUDNORM: process.env.LOUDNORM !== "0",
  LOUDNORM_I: process.env.LOUDNORM_I || "-16",

  PROBE_BIN: process.env.PROBE_BIN || "ffprobe",
  PROBE_CONTAINER: process.env.PROBE_CONTAINER === "1",
  SOURCE_HEIGHT_OVERRIDE: process.env.SOURCE_HEIGHT
    ? Number(process.env.SOURCE_HEIGHT)
    : null,

  LADDER: [
    { h: 2160, crf: 19, maxrate: "16000k", bufsize: "32000k", abr: "192k" },
    { h: 1440, crf: 19, maxrate: "11000k", bufsize: "22000k", abr: "192k" },
    { h: 1080, crf: 20, maxrate: "8000k", bufsize: "16000k", abr: "192k" },
    { h: 720, crf: 20, maxrate: "4500k", bufsize: "9000k", abr: "128k" },
    { h: 540, crf: 21, maxrate: "2500k", bufsize: "5000k", abr: "128k" },
    { h: 360, crf: 22, maxrate: "1200k", bufsize: "2400k", abr: "96k" },
    { h: 240, crf: 23, maxrate: "700k", bufsize: "1400k", abr: "64k" },
  ],

  R2: {
    accountId: process.env.R2_ACCOUNT_ID,
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    bucket: process.env.R2_BUCKET,
    publicBaseUrl: process.env.R2_PUBLIC_BASE_URL,
  },
  SOURCE_BUCKET: process.env.R2_SOURCE_BUCKET || process.env.R2_BUCKET,
};

// Vocabulary Mapping for Directus
export const STATUS = {
  QUEUED: "queued",
  DOWNLOADING: "downloading",
  TRANSCODING: "transcoding",
  PACKAGING: "packaging",
  UPLOADING: "uploading",
  COMPLETED: "completed",
  FAILED: "failed",
};

// Container-to-Host Path Translation Function
export const toHost = (p) =>
  p.replace(Config.WORKSPACE_CONTAINER, Config.WORKSPACE_HOST);

// ============================================================================
// FFMPMPEG COMMAND LAYOUT ENGINE
// ============================================================================
export const Commands = {
  buildLadder: (srcHeight) => {
    let cap = Math.min(srcHeight, 2160);
    cap = cap - (cap % 2);
    let rungs = Config.LADDER.filter((r) => r.h <= cap);
    if (rungs.length === 0) rungs = [Config.LADDER[Config.LADDER.length - 1]];
    if (cap > rungs[0].h * 1.05) {
      const tier = Config.LADDER.find((r) => r.h >= cap) || Config.LADDER[0];
      rungs = [{ ...tier, h: cap }, ...rungs];
    }
    return rungs;
  },

  encodeCommand: (rung, fps, hasAudio) => {
    const gop = Math.max(2, Math.round((fps || 30) * Config.SEGMENT_SEC));
    const parts = [
      "-y -i ${INPUT_FILE}",
      `-vf scale=-2:${rung.h}:flags=lanczos`,
    ];

    if (Config.ENCODER_ENGINE === "amf") {
      // AMD AMF recommendations for high quality VOD:
      // Uses Peak Variable Bitrate (vbr_peak), and optimizes target to 85% of peak maximum
      const numericMax = parseInt(rung.maxrate);
      const targetBitrate = Math.floor(numericMax * 0.85) + "k";

      parts.push(
        `-c:v h264_amf`,
        `-quality quality`, // Quality usage target preset
        `-profile:v high`,
        `-rc vbr_peak`, // Peak VBR (Preferred mode over CBR for non-live files)
        `-b:v ${targetBitrate}`,
        `-maxrate ${rung.maxrate}`,
      );
    } else {
      // Classic Software CPU libx264 Fallback
      parts.push(
        `-c:v libx264 -preset ${Config.X264_PRESET} -profile:v high -pix_fmt yuv420p`,
        `-crf ${rung.crf} -maxrate ${rung.maxrate} -bufsize ${rung.bufsize}`,
        `-x264-params "keyint=${gop}:min-keyint=${gop}:scenecut=0:open_gop=0"`,
      );
    }

    // Force keyframes at uniform bounds for proper GOP alignment
    parts.push(
      `-force_key_frames "expr:gte(t,n_forced*${Config.SEGMENT_SEC})"`,
    );

    if (hasAudio) {
      parts.push(`-c:a aac -ac 2 -ar 48000 -b:a ${rung.abr}`);
      if (Config.LOUDNORM)
        parts.push(`-af loudnorm=I=${Config.LOUDNORM_I}:TP=-1.5:LRA=11`);
    } else {
      parts.push("-an");
    }

    parts.push("-movflags +faststart", "${OUTPUT_FILE}");
    return parts.join(" ");
  },

  hlsCommand: (ctx, rungs, hasAudio) => {
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
      "-max_muxing_queue_size 1024", // Alleviates muxing context buffer exhaustion errors at progress=end
      "-hls_segment_type fmp4", // Switches from TS to fMP4 to eliminate stream copy Annex B bitstream faults
      `-hls_segment_filename "${ctx.workdir}/hls/v%v/seg_%03d.m4s"`, // Changes segment extensions to standard .m4s
      `"${ctx.workdir}/hls/v%v/index.m3u8"`,
    ].join(" ");
  },
};
