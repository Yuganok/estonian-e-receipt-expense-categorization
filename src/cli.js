#!/usr/bin/env node

const path = require('path');
const { runMaxima } = require('./providers/maxima');
const { ensureDir } = require('./utils/fs');
const { parseArgs } = require('./utils/args');
const { log } = require('./utils/log');

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (args.help) {
    process.stdout.write(getHelp());
    return;
  }

  const provider = (args.provider || 'maxima').toLowerCase();
  if (provider !== 'maxima') {
    throw new Error(`Unknown provider: ${provider}. Currently only maxima is supported.`);
  }

  const from = args.from || null;
  const to = args.to || null;
  const outDir = path.resolve(process.cwd(), args.out || 'receipts');
  const headless = Boolean(args.headless);
  const slowMo = args.slowmo != null ? Number(args.slowmo) : 0;
  const debug = Boolean(args.debug);
  const showBrowserAllSteps = Boolean(args['show-browser-all-steps']);

  if ((from && !isIsoDate(from)) || (to && !isIsoDate(to))) {
    throw new Error('Dates must be in YYYY-MM-DD format (for example 2026-01-31).');
  }
  if (!from || !to) {
    throw new Error('Both --from and --to are required (YYYY-MM-DD).');
  }

  // Strict validation: date must exist in the calendar (JS would otherwise roll to next month).
  assertRealIsoDate(from, '--from');
  assertRealIsoDate(to, '--to');
  if (from > to) {
    throw new Error(`Invalid date range: from (${from}) is later than to (${to}).`);
  }

  await ensureDir(outDir);

  log.info(`Provider: ${provider}`);
  log.info(`Period: ${from} .. ${to}`);
  log.info(`Output: ${outDir}`);
  log.info(`Headless: ${headless ? 'yes' : 'no'}`);
  log.info(`Show browser all steps: ${showBrowserAllSteps ? 'yes' : 'no'}`);

  await runMaxima({
    from,
    to,
    outDir,
    headless,
    slowMo,
    debug,
    showBrowserAllSteps
  });
}

function isIsoDate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function assertRealIsoDate(iso, argName) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso);
  if (!m) throw new Error(`Invalid date format for ${argName}: ${iso}`);
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  const dt = new Date(y, mo - 1, d);
  if (dt.getFullYear() !== y || dt.getMonth() !== mo - 1 || dt.getDate() !== d) {
    throw new Error(
      `Non-existent date for ${argName}: ${iso}. ` +
      `For example, for February use 2026-02-28 (or 2026-02-29 in a leap year).`
    );
  }
}

function getHelp() {
  return `
Download e-receipts (Maxima EE)

Usage:
  node src/cli.js --provider maxima --from YYYY-MM-DD --to YYYY-MM-DD [--out receipts] [--headless] [--slowmo 250] [--debug] [--show-browser-all-steps]

Parameters:
  --provider   maxima (default: maxima)
  --from       start date (YYYY-MM-DD)
  --to         end date (YYYY-MM-DD)
  --out        output directory (default: receipts)
  --headless   run without UI (not recommended for Smart-ID)
  --slowmo     delay between actions in ms
  --debug      verbose logs and debug artifacts on errors
  --show-browser-all-steps  legacy mode: browser stays visible for all steps
  --help       show help
`;
}

main().catch((err) => {
  process.stderr.write((err && err.stack) ? String(err.stack) : String(err));
  process.stderr.write('\n');
  process.exitCode = 1;
});

