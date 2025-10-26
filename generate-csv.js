#!/usr/bin/env node
/**
 * Generate CSV and CSV.GZ with batched writes.
 * Fields:
 *  - accountNumber: 14-digit numeric (strictly < 15 chars)
 *  - productCode: 3-char alphanumeric uppercase (strictly < 4 chars)
 *
 * Usage:
 *   node generate-csv.js --count=10000000 --batch=10000 --out=accounts.csv
 *
 * Defaults:
 *   count = 1_000_000
 *   batch = 50_000
 *   out   = output.csv
 */

const fs = require("fs");
const path = require("path");
const { once } = require("events");
const zlib = require("zlib");

// ---- CLI args ----
const arg = (name, def) => {
  const m = process.argv.find((a) => a.startsWith(`--${name}=`));
  return m ? m.split("=")[1] : def;
};
const COUNT = Number(arg("count", "1000000"));
const BATCH = Math.max(1, Number(arg("batch", "50000")));
const OUT = arg("out", "output.csv");

// Ensure dirs exist
fs.mkdirSync(path.dirname(OUT), { recursive: true });

// ---- Streams: plain CSV and gzipped CSV ----
const csvStream = fs.createWriteStream(OUT, { encoding: "utf8" });
const gzStream = zlib.createGzip({ level: zlib.constants.Z_BEST_SPEED });
const gzOut = fs.createWriteStream(`${OUT}.gz`);
gzStream.pipe(gzOut);

// Backpressure-aware write
async function writeBoth(chunk) {
  const ok1 = csvStream.write(chunk);
  const ok2 = gzStream.write(chunk);
  if (!ok1) await once(csvStream, "drain");
  if (!ok2) await once(gzStream, "drain");
}

// Random generators (fast, deterministic length)
function randDigits(len) {
  // Generate len digits, first digit nonzero
  let s = String(Math.floor(Math.random() * 9) + 1);
  while (s.length < len) {
    const n = Math.floor(Math.random() * 10);
    s += n.toString();
  }
  return s;
}

const ALPHANUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
function randAlphaNum(len) {
  let s = "";
  for (let i = 0; i < len; i++) {
    s += ALPHANUM[(Math.random() * ALPHANUM.length) | 0];
  }
  return s;
}

// CSV escape (not strictly needed here, but safe)
function csvCell(s) {
  if (/[",\n]/.test(s)) return `"${String(s).replace(/"/g, '""')}"`;
  return s;
}

(async () => {
  console.time("generate");

  // Write headers once to both outputs
  await writeBoth("accountNumber,productCode\n");

  let written = 0;
  while (written < COUNT) {
    const size = Math.min(BATCH, COUNT - written);
    const rows = new Array(size);

    for (let i = 0; i < size; i++) {
      const accountNumber = randDigits(14); // <15 characters
      const productCode = randAlphaNum(3); // <4 characters
      rows[i] = `${csvCell(accountNumber)},${csvCell(productCode)}`;
    }

    const chunk = rows.join("\n") + "\n";
    await writeBoth(chunk);

    written += size;
    // Lightweight progress log every ~1e5 rows
    if (written % 100000 === 0 || written === COUNT) {
      console.log(
        `Generated ${written.toLocaleString()} / ${COUNT.toLocaleString()}`
      );
    }
  }

  // Close streams cleanly
  csvStream.end();
  gzStream.end();

  await Promise.all([once(csvStream, "finish"), once(gzOut, "finish")]);

  console.timeEnd("generate");
  console.log(`Done:
  - CSV:     ${path.resolve(OUT)}
  - CSV.GZ:  ${path.resolve(OUT + ".gz")}`);
})().catch((err) => {
  console.error("Generation failed:", err);
  process.exit(1);
});
