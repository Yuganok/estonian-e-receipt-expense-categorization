const { app, BrowserWindow, dialog, ipcMain } = require('electron');
const fs = require('fs').promises;
const fsNative = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const BANK_EXPORTS_DIR = 'bank_exports';
const OUTPUT_DIR = 'output';
const OUTPUT_SESSIONS_DIR = path.join(OUTPUT_DIR, 'sessions');
const MEMORY_DB_NAME = 'manual_category_memory.db';
let latestAnalysisOutDir = null;

function repoRoot() {
  return path.resolve(__dirname, '..');
}

function analysisRoot(root = repoRoot()) {
  return path.join(root, 'analysis');
}

function memoryDbPath(root = repoRoot()) {
  return path.join(root, OUTPUT_DIR, MEMORY_DB_NAME);
}

function resolvePath(p) {
  if (!p || !String(p).trim()) return repoRoot();
  const s = String(p).trim();
  return path.isAbsolute(s) ? s : path.join(repoRoot(), s);
}

function makeSessionId(now = new Date()) {
  const y = now.getFullYear();
  const mo = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  const hh = String(now.getHours()).padStart(2, '0');
  const mm = String(now.getMinutes()).padStart(2, '0');
  const ss = String(now.getSeconds()).padStart(2, '0');
  return `${y}${mo}${d}-${hh}${mm}${ss}`;
}

function createSessionOutputDir(root) {
  const sessionsRoot = path.join(root, OUTPUT_SESSIONS_DIR);
  fsNative.mkdirSync(sessionsRoot, { recursive: true });
  const dir = path.join(sessionsRoot, makeSessionId());
  fsNative.mkdirSync(dir, { recursive: true });
  return dir;
}

async function findLatestSessionOutputDir(root) {
  const sessionsRoot = path.join(root, OUTPUT_SESSIONS_DIR);
  let entries = [];
  try {
    entries = await fs.readdir(sessionsRoot, { withFileTypes: true });
  } catch {
    return null;
  }

  const dirs = entries.filter((e) => e.isDirectory()).map((e) => e.name).sort().reverse();
  if (!dirs.length) return null;
  return path.join(sessionsRoot, dirs[0]);
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1000,
    height: 820,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  win.loadFile(path.join(__dirname, 'renderer', 'index.html'));
}

function pipeProcess(child, webContents, afterClose) {
  const send = (chunk) => webContents.send('job-log', chunk);
  let collected = '';
  const collect = (d) => {
    const text = d.toString();
    collected += text;
    if (collected.length > 120000) {
      collected = collected.slice(-60000);
    }
    send(text);
  };
  child.stdout.on('data', collect);
  child.stderr.on('data', collect);
  child.on('error', (err) => send(`[spawn error] ${err.message}\n`));
  child.on('close', (code) => {
    if (code !== 0) {
      if (collected.includes('[phase1]')) {
        send('[error] Login phase failed. Please try again and complete Smart-ID confirmation in the browser.\n');
      } else if (collected.includes('[phase2]')) {
        send('[error] Background phase failed. Session may have expired, please run download again.\n');
      }
    }
    let payload = { code, ok: code === 0 };
    if (typeof afterClose === 'function') {
      try {
        const afterPayload = afterClose(code);
        if (afterPayload && typeof afterPayload === 'object') {
          payload = { ...payload, ...afterPayload };
        }
      } catch (e) {
        send(`${String(e)}\n`);
      }
    }
    webContents.send('job-done', payload);
  });
}

const RIMI_DOWNLOAD_HINT =
  '[INFO] Rimi automatic download is not available. Add files manually to receipts/Rimi/ (optionally with rimi_manual.csv).\n';

function runDownload(webContents, opts) {
  const mode = String(opts.stores || 'all').toLowerCase();
  if (mode === 'rimi') {
    webContents.send('job-log', RIMI_DOWNLOAD_HINT);
    webContents.send('job-done', { code: 0, ok: true });
    return;
  }

  const root = repoRoot();
  const cli = path.join(root, 'src', 'cli.js');
  const outDir = resolvePath('receipts');
  webContents.send('job-log', '[phase] Log in in the visible browser. After successful confirmation, download continues in background mode.\n');
  const args = [
    cli,
    '--provider',
    'maxima',
    '--from',
    opts.from,
    '--to',
    opts.to,
    '--out',
    outDir
  ];

  const child = spawn('node', args, {
    cwd: root,
    env: process.env,
    shell: false
  });

  const afterClose =
    mode === 'all'
      ? (code) => {
          if (code === 0) {
            webContents.send('job-log', `\n${RIMI_DOWNLOAD_HINT}`);
          }
        }
      : undefined;
  pipeProcess(child, webContents, afterClose);
}

function isSafeBankCsvPath(p) {
  const s = String(p || '').trim();
  if (!s) return false;
  if (!path.isAbsolute(s)) return false;
  if (!s.toLowerCase().endsWith('.csv')) return false;
  return fsNative.existsSync(s);
}

function runPipeline(webContents, opts) {
  const root = repoRoot();
  const analysisDir = analysisRoot(root);
  const memoryDb = memoryDbPath(root);
  const receipts = resolvePath('receipts');
  const mode = String(opts.mode || 'full').trim().toLowerCase();
  const isSingleMode = mode === 'single';
  const bankCsvPath = String(opts.bankCsvPath || '').trim();
  const sessionOut = createSessionOutputDir(root);
  const stores = String(opts.stores || 'all').toLowerCase();
  const storesArg = stores === 'maxima' || stores === 'rimi' || stores === 'all' ? stores : 'all';
  const maximaPurchasesCsv = path.join(receipts, 'Maxima', 'purchases.csv');
  let bank = '';
  let bankCoverage = null;
  let singleReceiptPdf = '';
  let singleReceiptStore = '';

  if (isSingleMode) {
    singleReceiptPdf = String(opts.singleReceiptPdfPath || '').trim();
    singleReceiptStore = String(opts.singleReceiptStore || 'Selver').trim() || 'Selver';
    if (!singleReceiptPdf || !path.isAbsolute(singleReceiptPdf) || !singleReceiptPdf.toLowerCase().endsWith('.pdf')) {
      webContents.send('job-log', '[error] Select a valid absolute PDF path for single-receipt analysis.\n');
      webContents.send('job-done', { code: 1, ok: false });
      return;
    }
    if (!fsNative.existsSync(singleReceiptPdf)) {
      webContents.send('job-log', `[error] Single receipt PDF not found: ${singleReceiptPdf}\n`);
      webContents.send('job-done', { code: 1, ok: false });
      return;
    }
  } else {
    if (bankCsvPath && !isSafeBankCsvPath(bankCsvPath)) {
      webContents.send('job-log', '[error] Selected bank CSV path is invalid or missing.\n');
      webContents.send('job-done', { code: 1, ok: false });
      return;
    }
    bank = bankCsvPath;
    if (bank) {
      const receiptDateRange =
        storesArg !== 'rimi' ? safeReadDateRangeFromPurchasesCsv(maximaPurchasesCsv) : null;
      const bankDateRange = safeReadDateRangeFromBankCsv(bank);
      bankCoverage = analyzeDateCoverage(receiptDateRange, bankDateRange);
    }
  }

  const args = [
    '-3',
    'main.py',
    '--receipts',
    receipts,
    '--out',
    sessionOut,
    '--memory-db',
    memoryDb,
    '--stores',
    storesArg
  ];

  if (isSingleMode) {
    args.push('--single-receipt-pdf', singleReceiptPdf);
    args.push('--single-receipt-store', singleReceiptStore);
    args.push('--skip-bank-match');
    webContents.send('job-log', `[info] Single-receipt mode: ${singleReceiptPdf}\n`);
    webContents.send('job-log', `[info] Receipt store label: ${singleReceiptStore}\n`);
  } else {
    if (bank) {
      args.push('--bank', bank);
    } else {
      args.push('--skip-bank-match');
      webContents.send('job-log', '[info] Full mode without bank CSV: bank matching will be skipped.\n');
    }
  }

  if (!isSingleMode && storesArg !== 'rimi' && fsNative.existsSync(maximaPurchasesCsv)) {
    args.push('--maxima-purchases-csv', maximaPurchasesCsv);
    webContents.send(
      'job-log',
      `[info] Analysis will use only Maxima receipts from this session: ${maximaPurchasesCsv}\n`
    );
  } else if (!isSingleMode && storesArg !== 'rimi') {
    webContents.send(
      'job-log',
      '[warn] Maxima purchases.csv was not found — analysis will process all Maxima receipts from receipts/Maxima.\n'
    );
  }
  if (!isSingleMode && bank) {
    emitBankCoverageGuardrail(webContents, bankCoverage, path.basename(bank));
  }
  webContents.send('job-log', `[info] Session output directory: ${sessionOut}\n`);
  const child = spawn('py', args, {
    cwd: analysisDir,
    env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    shell: false
  });
  pipeProcess(child, webContents, (code) => {
    if (code === 0) {
      latestAnalysisOutDir = sessionOut;
      writeSessionRunArtifacts(sessionOut, {
        from: String(opts.from || '').trim(),
        to: String(opts.to || '').trim(),
        stores: storesArg,
        mode: isSingleMode ? 'single' : 'full',
        bankBasename: bank ? path.basename(bank) : '',
        bankPath: bank,
        singleReceiptPdfPath: singleReceiptPdf,
        singleReceiptStore,
        maximaPurchasesCsvUsed:
          !isSingleMode && storesArg !== 'rimi' && fsNative.existsSync(maximaPurchasesCsv)
            ? maximaPurchasesCsv
            : '',
        bankCoverage
      });
    }
    return { outputDir: sessionOut };
  });
}

function parseDelimitedLine(line, delimiter) {
  const cells = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === delimiter && !inQuotes) {
      cells.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  cells.push(current);
  return cells.map((v) => v.trim());
}

function parseCsvLine(line) {
  return parseDelimitedLine(line, ',');
}

function toNumber(value) {
  const n = Number(String(value || '').replace(',', '.'));
  return Number.isFinite(n) ? n : 0;
}

function pad2(value) {
  return String(value).padStart(2, '0');
}

function formatYmdDate(date) {
  return `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;
}

function parseFlexibleDate(value) {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const datePart = raw.replace(/^"+|"+$/g, '').split(',')[0].trim();

  const dmy = /^(\d{2})\.(\d{2})\.(\d{2}|\d{4})$/.exec(datePart);
  if (dmy) {
    const day = Number(dmy[1]);
    const month = Number(dmy[2]);
    let year = Number(dmy[3]);
    if (dmy[3].length === 2) {
      year += year >= 70 ? 1900 : 2000;
    }
    const date = new Date(Date.UTC(year, month - 1, day));
    if (
      date.getUTCFullYear() === year &&
      date.getUTCMonth() === month - 1 &&
      date.getUTCDate() === day
    ) {
      return date;
    }
  }

  const ymd = /^(\d{4})-(\d{2})-(\d{2})$/.exec(datePart);
  if (ymd) {
    const year = Number(ymd[1]);
    const month = Number(ymd[2]);
    const day = Number(ymd[3]);
    const date = new Date(Date.UTC(year, month - 1, day));
    if (
      date.getUTCFullYear() === year &&
      date.getUTCMonth() === month - 1 &&
      date.getUTCDate() === day
    ) {
      return date;
    }
  }
  return null;
}

function getDateRangeStats(dates) {
  if (!Array.isArray(dates) || dates.length === 0) return null;
  const timestamps = dates.map((d) => d.getTime());
  const minTs = Math.min(...timestamps);
  const maxTs = Math.max(...timestamps);
  return {
    count: dates.length,
    minDate: new Date(minTs),
    maxDate: new Date(maxTs)
  };
}

function safeReadDateRangeFromPurchasesCsv(filePath) {
  if (!fsNative.existsSync(filePath)) {
    return { ok: false, reason: 'missing-purchases-csv' };
  }
  try {
    const text = fsNative.readFileSync(filePath, 'utf-8');
    const lines = text
      .replace(/\r/g, '')
      .split('\n')
      .filter((l) => l.trim().length > 0);
    if (lines.length < 2) return { ok: false, reason: 'empty-purchases-csv' };
    const headers = parseCsvLine(lines[0]);
    const dateIdx = headers.findIndex((h) => h === 'paymentDate');
    if (dateIdx < 0) return { ok: false, reason: 'missing-paymentDate-column' };

    const dates = [];
    let invalidRows = 0;
    for (const line of lines.slice(1)) {
      const cols = parseCsvLine(line);
      const dt = parseFlexibleDate(cols[dateIdx] || '');
      if (dt) dates.push(dt);
      else invalidRows += 1;
    }
    const range = getDateRangeStats(dates);
    if (!range) return { ok: false, reason: 'no-valid-receipt-dates', invalidRows };
    return { ok: true, ...range, invalidRows };
  } catch (error) {
    return {
      ok: false,
      reason: 'read-error',
      error: String(error && error.message ? error.message : error)
    };
  }
}

function safeReadDateRangeFromBankCsv(filePath) {
  if (!fsNative.existsSync(filePath)) {
    return { ok: false, reason: 'missing-bank-csv' };
  }
  try {
    const text = fsNative.readFileSync(filePath, 'utf-8').replace(/^\uFEFF/, '');
    const lines = text
      .replace(/\r/g, '')
      .split('\n')
      .filter((l) => l.trim().length > 0);
    if (lines.length < 2) return { ok: false, reason: 'empty-bank-csv' };
    const headers = parseDelimitedLine(lines[0], ';').map((h) => h.replace(/^"+|"+$/g, ''));
    const dateIdx = headers.findIndex((h) => h === 'Kuupäev');
    if (dateIdx < 0) return { ok: false, reason: 'missing-Kuupaev-column' };
    const dcIdx = headers.findIndex((h) => h === 'Deebet/Kreedit');

    const dates = [];
    let invalidRows = 0;
    for (const line of lines.slice(1)) {
      const cols = parseDelimitedLine(line, ';').map((c) => c.replace(/^"+|"+$/g, ''));
      const dc = dcIdx >= 0 ? String(cols[dcIdx] || '').trim() : '';
      if (dcIdx >= 0 && dc !== 'D') continue;
      const dt = parseFlexibleDate(cols[dateIdx] || '');
      if (dt) dates.push(dt);
      else invalidRows += 1;
    }
    const range = getDateRangeStats(dates);
    if (!range) return { ok: false, reason: 'no-valid-bank-dates', invalidRows };
    return { ok: true, ...range, invalidRows };
  } catch (error) {
    return {
      ok: false,
      reason: 'read-error',
      error: String(error && error.message ? error.message : error)
    };
  }
}

function analyzeDateCoverage(receiptRange, bankRange) {
  if (!receiptRange || !receiptRange.ok) {
    return {
      ok: false,
      reason: receiptRange ? receiptRange.reason : 'receipt-range-unavailable',
      receiptRange,
      bankRange
    };
  }
  if (!bankRange || !bankRange.ok) {
    return {
      ok: false,
      reason: bankRange ? bankRange.reason : 'bank-range-unavailable',
      receiptRange,
      bankRange
    };
  }

  const dayMs = 24 * 60 * 60 * 1000;
  const receiptMinTs = receiptRange.minDate.getTime();
  const receiptMaxTs = receiptRange.maxDate.getTime();
  const bankMinTs = bankRange.minDate.getTime();
  const bankMaxTs = bankRange.maxDate.getTime();
  const receiptDays = Math.max(1, Math.floor((receiptMaxTs - receiptMinTs) / dayMs) + 1);

  const strictOverlapStart = Math.max(receiptMinTs, bankMinTs);
  const strictOverlapEnd = Math.min(receiptMaxTs, bankMaxTs);
  const strictOverlapDays =
    strictOverlapStart <= strictOverlapEnd
      ? Math.floor((strictOverlapEnd - strictOverlapStart) / dayMs) + 1
      : 0;
  const strictCoveragePct = (strictOverlapDays / receiptDays) * 100;

  const shiftedBankMinTs = bankMinTs - 2 * dayMs;
  const shiftedBankMaxTs = bankMaxTs + 2 * dayMs;
  const shiftedOverlapStart = Math.max(receiptMinTs, shiftedBankMinTs);
  const shiftedOverlapEnd = Math.min(receiptMaxTs, shiftedBankMaxTs);
  const shiftedOverlapDays =
    shiftedOverlapStart <= shiftedOverlapEnd
      ? Math.floor((shiftedOverlapEnd - shiftedOverlapStart) / dayMs) + 1
      : 0;
  const shiftedCoveragePct = (shiftedOverlapDays / receiptDays) * 100;

  return {
    ok: true,
    strictCoveragePct: Number(strictCoveragePct.toFixed(1)),
    shiftedCoveragePct: Number(shiftedCoveragePct.toFixed(1)),
    receiptRange,
    bankRange,
    warning: shiftedCoveragePct < 90
  };
}

function emitBankCoverageGuardrail(webContents, coverage, bankBasename) {
  if (!coverage || !coverage.ok) {
    if (coverage && coverage.reason !== 'missing-purchases-csv') {
      webContents.send('job-log', `[info] Bank CSV coverage check skipped (${coverage.reason || 'unavailable'}).\n`);
    }
    return;
  }

  const receiptMin = formatYmdDate(coverage.receiptRange.minDate);
  const receiptMax = formatYmdDate(coverage.receiptRange.maxDate);
  const bankMin = formatYmdDate(coverage.bankRange.minDate);
  const bankMax = formatYmdDate(coverage.bankRange.maxDate);
  webContents.send(
    'job-log',
    `[info] Bank CSV date coverage check (${bankBasename}): receipts ${receiptMin}..${receiptMax}, bank debits ${bankMin}..${bankMax}, strict ${coverage.strictCoveragePct.toFixed(
      1
    )}%, with +/-2d shift ${coverage.shiftedCoveragePct.toFixed(1)}%.\n`
  );
  if (coverage.warning) {
    webContents.send(
      'job-log',
      '[warn] Bank CSV may not fully cover this receipt period. Matching may drop due to date range mismatch.\n'
    );
  }
}

function writeSessionRunArtifacts(sessionOut, meta) {
  try {
    const researchDir = path.join(sessionOut, 'research');
    fsNative.mkdirSync(researchDir, { recursive: true });

    const manifest = {
      generatedAt: new Date().toISOString(),
      sessionOutDir: sessionOut,
      stores: meta.stores || 'all',
      mode: meta.mode || 'full',
      selectedPeriod: {
        from: meta.from || '',
        to: meta.to || ''
      },
      bankCsv: {
        basename: meta.bankBasename || '',
        path: meta.bankPath || ''
      },
      singleReceipt: {
        pdfPath: meta.singleReceiptPdfPath || '',
        store: meta.singleReceiptStore || ''
      },
      maximaPurchasesCsvUsed: meta.maximaPurchasesCsvUsed || '',
      bankCoverage: meta.bankCoverage || null
    };
    fsNative.writeFileSync(
      path.join(researchDir, 'run_manifest.json'),
      `${JSON.stringify(manifest, null, 2)}\n`,
      'utf-8'
    );

    const quickMetricsText = [
      'QUICK METRICS DEFINITIONS',
      '',
      'This project uses quick metrics as an internal pilot evaluation, not as a final gold-standard benchmark.',
      '',
      '- quick accuracy: share of rows in the reviewed quick sample where predicted category equals reviewed category.',
      '- quick macro F1: unweighted mean of per-category F1 on the reviewed quick sample.',
      '- quick spend error: share of non-deposit spend in the quick sample that belongs to misclassified rows.',
      '',
      'Interpretation note:',
      '- Treat quick metrics as a fast stability signal (kiire hindamine / sisemine kontroll / pilootmoodik).',
      '- For thesis reporting, clearly separate quick metrics from full manual gold evaluation.'
    ].join('\n');
    fsNative.writeFileSync(path.join(researchDir, 'quick_metrics_definition.txt'), `${quickMetricsText}\n`, 'utf-8');
  } catch {
    // Best-effort artifact writing; analysis results should still succeed.
  }
}

async function readCsvRows(filePath) {
  let text;
  try {
    text = await fs.readFile(filePath, 'utf-8');
  } catch {
    return null;
  }
  const lines = text.replace(/\r/g, '').split('\n').filter((l) => l.trim().length > 0);
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = cols[idx] ?? '';
    });
    return row;
  });
}

function csvEscape(value) {
  const s = String(value ?? '');
  if (/[",\r\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

async function writeCsvRows(filePath, headers, rows) {
  const lines = [headers.join(',')];
  for (const row of rows) {
    const line = headers.map((h) => csvEscape(row[h] ?? '')).join(',');
    lines.push(line);
  }
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${lines.join('\n')}\n`, 'utf-8');
}

function detectNoiseRow(itemText) {
  const t = String(itemText || '').trim();
  if (!t) return false;
  if (/^\d+\s+[\d,.]+\s*€\s+[\d,.]+\s*€$/i.test(t)) return true;
  if (/^\d+(?:[.,]\d+)?\s*(tk|kg|g|l|ml|pakk)$/i.test(t)) return true;
  return false;
}

function asBool(value) {
  const t = String(value ?? '').trim().toLowerCase();
  return t === 'true' || t === '1' || t === 'yes';
}

function parseManualCorrectionsRows(rows) {
  return (rows || []).map((r) => ({
    receipt_id: String(r.receipt_id || ''),
    item_text: String(r.item_text || ''),
    manual_category: String(r.manual_category || ''),
    note: String(r.note || ''),
    updated_at: String(r.updated_at || '')
  }));
}

function parseJsonFromStdout(stdoutText) {
  const text = String(stdoutText || '').trim();
  if (!text) return null;
  const lines = text.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  const candidates = lines.length ? [lines[lines.length - 1], text] : [text];
  for (const candidate of candidates) {
    try {
      return JSON.parse(candidate);
    } catch {
      // try next candidate
    }
  }
  return null;
}

async function runMemoryLayerCommand(args, dbPath) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      'py',
      ['-3', 'memory_layer.py', '--db', dbPath, ...args],
      {
        cwd: analysisRoot(),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
        shell: false
      }
    );
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => {
      stdout += d.toString();
    });
    child.stderr.on('data', (d) => {
      stderr += d.toString();
    });
    child.on('error', (err) => reject(err));
    child.on('close', (code) => {
      if (code === 0) {
        resolve(stdout.trim());
      } else {
        reject(new Error(`memory_layer exited with code ${code}: ${stderr || stdout}`));
      }
    });
  });
}

async function importLegacyCorrectionsIfNeeded(legacyCsvPath, dbPath) {
  const markerPath = `${legacyCsvPath}.sqlite_imported`;
  if (!fsNative.existsSync(legacyCsvPath) || fsNative.existsSync(markerPath)) {
    return { imported: 0, skipped: true };
  }
  const stdout = await runMemoryLayerCommand(['import-legacy-csv', '--csv', legacyCsvPath], dbPath);
  const parsed = parseJsonFromStdout(stdout) || {};
  await fs.writeFile(markerPath, `${new Date().toISOString()}\n`, 'utf-8');
  return {
    imported: Number(parsed.imported || 0),
    skipped: false
  };
}

async function loadManualCorrectionsFromMemory(itemsCategorizedPath, dbPath) {
  if (!fsNative.existsSync(itemsCategorizedPath)) return [];
  const stdout = await runMemoryLayerCommand(
    ['get-session-corrections', '--categorized-csv', itemsCategorizedPath, '--out-json', '-'],
    dbPath
  );
  const parsed = parseJsonFromStdout(stdout);
  if (!Array.isArray(parsed)) return [];
  return parseManualCorrectionsRows(parsed);
}

async function getResearchData(preferredOutDir) {
  const outDir = await resolveSummaryOutDir(preferredOutDir);
  const researchDir = path.join(outDir, 'research');
  const memoryDb = memoryDbPath();
  const itemsRawPath = path.join(outDir, 'items_raw.csv');
  const itemsCategorizedPath = path.join(outDir, 'items_categorized.csv');
  const matchedPath = path.join(outDir, 'matched.csv');
  const categoryBreakdownPath = path.join(outDir, 'category_breakdown.csv');
  const sourceBreakdownPath = path.join(outDir, 'category_source_breakdown.csv');
  const correctionsPath = path.join(researchDir, 'manual_corrections.csv');

  const [itemsRawRows, categorizedRows, matchedRows, breakdownRows, sourceRows, correctionRows] = await Promise.all([
    readCsvRows(itemsRawPath),
    readCsvRows(itemsCategorizedPath),
    readCsvRows(matchedPath),
    readCsvRows(categoryBreakdownPath),
    readCsvRows(sourceBreakdownPath),
    readCsvRows(correctionsPath)
  ]);

  const rowsRaw = (itemsRawRows || []).map((r) => ({
    ...r,
    is_noise: detectNoiseRow(r.item_text),
    is_deposit: asBool(r.is_deposit)
  }));
  const rowsCategorized = (categorizedRows || []).map((r) => ({
    ...r,
    is_noise: detectNoiseRow(r.item_text),
    is_deposit: asBool(r.is_deposit)
  }));
  let corrections = [];
  try {
    await importLegacyCorrectionsIfNeeded(correctionsPath, memoryDb);
    corrections = await loadManualCorrectionsFromMemory(itemsCategorizedPath, memoryDb);
  } catch {
    corrections = [];
  }
  if (!corrections.length) {
    corrections = parseManualCorrectionsRows(correctionRows || []);
  }

  const receiptsTotal = (matchedRows || []).length;
  const receiptsUnmatched = (matchedRows || []).filter(
    (r) => String(r.match_confidence || '').toLowerCase() === 'none'
  ).length;
  const mismatchRatePct =
    receiptsTotal > 0 ? Number(((receiptsUnmatched / receiptsTotal) * 100).toFixed(1)) : 0;

  // Mixed receipts metric based on categorized rows (excluding deposits).
  const receiptCats = new Map();
  for (const row of rowsCategorized) {
    if (row.is_deposit) continue;
    const rid = String(row.receipt_id || '');
    if (!rid) continue;
    const cat = String(row.category || '').trim() || 'Unknown';
    if (!receiptCats.has(rid)) receiptCats.set(rid, new Set());
    receiptCats.get(rid).add(cat);
  }
  let mixedReceipts = 0;
  for (const cats of receiptCats.values()) {
    if (cats.size >= 2) mixedReceipts += 1;
  }

  return {
    ok: true,
    outputDir: outDir,
    researchDir,
    files: {
      items_raw: Boolean(itemsRawRows),
      items_categorized: Boolean(categorizedRows),
      matched: Boolean(matchedRows),
      category_breakdown: Boolean(breakdownRows),
      category_source_breakdown: Boolean(sourceRows),
      manual_corrections: Boolean(corrections.length) || Boolean(correctionRows)
    },
    itemsRaw: rowsRaw,
    itemsCategorized: rowsCategorized,
    matched: matchedRows || [],
    categoryBreakdown: (breakdownRows || []).filter(
      (r) => String(r.category || '').toUpperCase() !== 'TOTAL'
    ),
    categorySourceBreakdown: (sourceRows || []).filter(
      (r) => String(r.category_source || '').toUpperCase() !== 'TOTAL'
    ),
    manualCorrections: corrections,
    metrics: {
      receiptsTotal,
      receiptsUnmatched,
      mismatchRatePct,
      mixedReceipts
    }
  };
}

async function saveManualCorrections(preferredOutDir, corrections) {
  const outDir = await resolveSummaryOutDir(preferredOutDir);
  const researchDir = path.join(outDir, 'research');
  const categorizedPath = path.join(outDir, 'items_categorized.csv');
  const dbPath = memoryDbPath();
  const rows = Array.isArray(corrections) ? corrections : [];
  const normalized = rows
    .map((r) => ({
      receipt_id: String(r.receipt_id || ''),
      store: String(r.store || ''),
      item_text: String(r.item_text || ''),
      manual_category: String(r.manual_category || ''),
      note: String(r.note || ''),
      updated_at: String(r.updated_at || new Date().toISOString())
    }))
    .filter((r) => r.item_text && r.manual_category);
  if (!fsNative.existsSync(categorizedPath)) {
    return {
      ok: false,
      reason: 'missing-items-categorized',
      error: `Missing file: ${categorizedPath}`
    };
  }
  await fs.mkdir(researchDir, { recursive: true });
  const tmpJsonPath = path.join(researchDir, `manual_corrections_payload_${Date.now()}_${process.pid}.json`);
  await fs.writeFile(tmpJsonPath, `${JSON.stringify(normalized, null, 2)}\n`, 'utf-8');
  try {
    const stdout = await runMemoryLayerCommand(
      ['sync-session-corrections', '--categorized-csv', categorizedPath, '--corrections-json', tmpJsonPath],
      dbPath
    );
    const parsed = parseJsonFromStdout(stdout) || {};
    return {
      ok: true,
      outputDir: outDir,
      dbPath,
      saved: normalized.length,
      upserted: Number(parsed.upserted || 0),
      deleted: Number(parsed.deleted || 0)
    };
  } finally {
    try {
      await fs.unlink(tmpJsonPath);
    } catch {
      // noop
    }
  }
}

async function exportResearchReport(preferredOutDir, payload = {}) {
  const data = await getResearchData(preferredOutDir);
  const corrections = parseManualCorrectionsRows(payload.manualCorrections || []);
  const correctionMap = new Map(
    corrections.map((c) => [`${c.receipt_id}__${c.item_text}`, c])
  );

  const correctedRows = data.itemsCategorized.map((r) => {
    const key = `${String(r.receipt_id || '')}__${String(r.item_text || '')}`;
    const c = correctionMap.get(key);
    const finalCategory = c && c.manual_category ? c.manual_category : String(r.category || '');
    return {
      receipt_id: String(r.receipt_id || ''),
      store: String(r.store || ''),
      purchase_date: String(r.purchase_date || ''),
      item_text: String(r.item_text || ''),
      category_auto: String(r.category || ''),
      category_manual: c ? String(c.manual_category || '') : '',
      category_final: finalCategory,
      category_rule: String(r.category_rule || ''),
      category_source: String(r.category_source || ''),
      gross_price: String(r.gross_price || ''),
      discount: String(r.discount || ''),
      net_price: String(r.net_price || ''),
      is_deposit: String(r.is_deposit || ''),
      is_noise: String(r.is_noise || '')
    };
  });

  const researchDir = path.join(data.outputDir, 'research');
  await fs.mkdir(researchDir, { recursive: true });
  const csvPath = path.join(researchDir, 'research_ui_rows.csv');
  await writeCsvRows(
    csvPath,
    [
      'receipt_id',
      'store',
      'purchase_date',
      'item_text',
      'category_auto',
      'category_manual',
      'category_final',
      'category_rule',
      'category_source',
      'gross_price',
      'discount',
      'net_price',
      'is_deposit',
      'is_noise'
    ],
    correctedRows
  );

  const txtPath = path.join(researchDir, 'research_ui_report.txt');
  const lines = [
    'RESEARCH UI REPORT',
    `Generated: ${new Date().toISOString()}`,
    `Session output: ${data.outputDir}`,
    '',
    `Rows parsed: ${data.itemsRaw.length}`,
    `Rows categorized: ${data.itemsCategorized.length}`,
    `Receipts total: ${data.metrics.receiptsTotal}`,
    `Receipts unmatched: ${data.metrics.receiptsUnmatched}`,
    `Mismatch rate: ${data.metrics.mismatchRatePct.toFixed(1)}%`,
    `Mixed receipts: ${data.metrics.mixedReceipts}`,
    `Manual corrections: ${corrections.length}`,
    '',
    'Category breakdown (amount_eur / share_pct / row_count):'
  ];
  for (const row of data.categoryBreakdown) {
    lines.push(
      `- ${String(row.category || 'Unknown')}: ${String(row.amount_eur || '')} / ${String(
        row.share_pct || ''
      )}% / ${String(row.row_count || '')}`
    );
  }
  lines.push('');
  lines.push('Category source breakdown (amount_eur / share_pct_spend / row_count / share_pct_rows):');
  for (const row of data.categorySourceBreakdown || []) {
    lines.push(
      `- ${String(row.category_source || 'Unknown')}: ${String(row.amount_eur || '')} / ${String(
        row.share_pct_spend || ''
      )}% / ${String(row.row_count || '')} / ${String(row.share_pct_rows || '')}%`
    );
  }
  await fs.writeFile(txtPath, `${lines.join('\n')}\n`, 'utf-8');

  return {
    ok: true,
    outputDir: data.outputDir,
    researchDir,
    reportTxtPath: txtPath,
    rowsCsvPath: csvPath
  };
}

async function resolveSummaryOutDir(preferredOutDir) {
  if (preferredOutDir && fsNative.existsSync(preferredOutDir)) {
    return preferredOutDir;
  }
  if (latestAnalysisOutDir && fsNative.existsSync(latestAnalysisOutDir)) {
    return latestAnalysisOutDir;
  }
  const latest = await findLatestSessionOutputDir(repoRoot());
  if (latest) return latest;
  return path.join(repoRoot(), OUTPUT_DIR);
}

async function getAnalysisSummary(preferredOutDir) {
  const root = repoRoot();
  const outDir = await resolveSummaryOutDir(preferredOutDir);
  const categoriesPath = path.join(outDir, 'category_breakdown.csv');
  const sourceBreakdownPath = path.join(outDir, 'category_source_breakdown.csv');
  const matchedPath = path.join(outDir, 'matched.csv');
  const [categoryRows, sourceRows, matchedRows] = await Promise.all([
    readCsvRows(categoriesPath),
    readCsvRows(sourceBreakdownPath),
    readCsvRows(matchedPath)
  ]);

  if (!categoryRows && !matchedRows) {
    return { ok: true, empty: true, reason: 'missing-files' };
  }

  const categories = (categoryRows || [])
    .filter((r) => String(r.category || '').toUpperCase() !== 'TOTAL')
    .map((r) => ({
      category: r.category || 'Unknown',
      amountEur: toNumber(r.amount_eur),
      sharePct: toNumber(r.share_pct),
      rowCount: Math.round(toNumber(r.row_count))
    }))
    .sort((a, b) => b.amountEur - a.amountEur);

  const categorySources = (sourceRows || [])
    .filter((r) => String(r.category_source || '').toUpperCase() !== 'TOTAL')
    .map((r) => ({
      categorySource: String(r.category_source || 'unknown'),
      amountEur: toNumber(r.amount_eur),
      sharePctSpend: toNumber(r.share_pct_spend),
      rowCount: Math.round(toNumber(r.row_count)),
      sharePctRows: toNumber(r.share_pct_rows)
    }))
    .sort((a, b) => b.rowCount - a.rowCount);

  const totalRow = (categoryRows || []).find((r) => String(r.category || '').toUpperCase() === 'TOTAL');
  const totalSpend = totalRow ? toNumber(totalRow.amount_eur) : categories.reduce((sum, c) => sum + c.amountEur, 0);
  const totalRows = totalRow
    ? Math.round(toNumber(totalRow.row_count))
    : categories.reduce((sum, c) => sum + c.rowCount, 0);

  const receiptsTotal = (matchedRows || []).length;
  const receiptsMatched = (matchedRows || []).filter(
    (r) => String(r.match_confidence || '').toLowerCase() !== 'none'
  ).length;
  const receiptsUnmatched = Math.max(0, receiptsTotal - receiptsMatched);
  const matchedRatePct = receiptsTotal > 0 ? (receiptsMatched / receiptsTotal) * 100 : 0;

  const topCategory = categories[0] || null;

  return {
    ok: true,
    empty: categories.length === 0 && receiptsTotal === 0,
    outputDir: outDir,
    kpi: {
      totalSpendEur: Number(totalSpend.toFixed(2)),
      totalRows,
      receiptsTotal,
      receiptsMatched,
      matchedRatePct: Number(matchedRatePct.toFixed(1)),
      topCategory: topCategory ? topCategory.category : null,
      topCategoryPct: topCategory ? Number(topCategory.sharePct.toFixed(1)) : 0
    },
    charts: {
      categories,
      categorySources,
      matchCoverage: {
        matched: receiptsMatched,
        unmatched: receiptsUnmatched
      }
    }
  };
}

app.whenReady().then(() => {
  createWindow();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

ipcMain.on('job-download', (event, opts) => {
  runDownload(event.sender, opts);
});

ipcMain.on('job-pipeline', (event, opts) => {
  runPipeline(event.sender, opts);
});

ipcMain.handle('list-bank-csv-files', async () => {
  const dir = path.join(repoRoot(), BANK_EXPORTS_DIR);
  await fs.mkdir(dir, { recursive: true });
  const names = await fs.readdir(dir);
  return names
    .filter((n) => n.toLowerCase().endsWith('.csv'))
    .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
});

ipcMain.handle('pick-single-receipt-pdf', async () => {
  const result = await dialog.showOpenDialog({
    properties: ['openFile'],
    filters: [{ name: 'PDF files', extensions: ['pdf'] }]
  });
  if (result.canceled || !result.filePaths || result.filePaths.length === 0) {
    return { ok: false, canceled: true };
  }
  return { ok: true, path: String(result.filePaths[0] || '') };
});

ipcMain.handle('pick-bank-csv', async () => {
  const result = await dialog.showOpenDialog({
    properties: ['openFile'],
    filters: [{ name: 'CSV files', extensions: ['csv'] }]
  });
  if (result.canceled || !result.filePaths || result.filePaths.length === 0) {
    return { ok: false, canceled: true };
  }
  return { ok: true, path: String(result.filePaths[0] || '') };
});

ipcMain.handle('get-analysis-summary', async (_event, payload) => {
  try {
    const preferredOutDir =
      payload && typeof payload === 'object' && payload.outputDir
        ? String(payload.outputDir)
        : undefined;
    return await getAnalysisSummary(preferredOutDir);
  } catch (error) {
    return {
      ok: false,
      empty: true,
      reason: 'error',
      error: String(error && error.message ? error.message : error)
    };
  }
});

ipcMain.handle('get-research-data', async (_event, payload) => {
  try {
    const preferredOutDir =
      payload && typeof payload === 'object' && payload.outputDir
        ? String(payload.outputDir)
        : undefined;
    return await getResearchData(preferredOutDir);
  } catch (error) {
    return {
      ok: false,
      reason: 'error',
      error: String(error && error.message ? error.message : error)
    };
  }
});

ipcMain.handle('save-manual-corrections', async (_event, payload) => {
  try {
    const preferredOutDir =
      payload && typeof payload === 'object' && payload.outputDir
        ? String(payload.outputDir)
        : undefined;
    const corrections =
      payload && typeof payload === 'object' && Array.isArray(payload.corrections)
        ? payload.corrections
        : [];
    return await saveManualCorrections(preferredOutDir, corrections);
  } catch (error) {
    return {
      ok: false,
      reason: 'error',
      error: String(error && error.message ? error.message : error)
    };
  }
});

ipcMain.handle('export-research-report', async (_event, payload) => {
  try {
    const preferredOutDir =
      payload && typeof payload === 'object' && payload.outputDir
        ? String(payload.outputDir)
        : undefined;
    return await exportResearchReport(preferredOutDir, payload || {});
  } catch (error) {
    return {
      ok: false,
      reason: 'error',
      error: String(error && error.message ? error.message : error)
    };
  }
});
