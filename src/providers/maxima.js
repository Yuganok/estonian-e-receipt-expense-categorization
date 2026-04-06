const fs = require('fs/promises');
const path = require('path');
const { chromium } = require('playwright');
const { ensureDir } = require('../utils/fs');
const { log } = require('../utils/log');
const { sleep, yyyyMm } = require('../utils/time');
const { toCsv } = require('../utils/csv');
const { withRetries } = require('../utils/retry');

/**
 * Maxima EE e-receipts downloader.
 *
 * Important: Smart-ID must be confirmed manually in a visible browser.
 * This module is responsible for:
 * - opening the browser
 * - trying to reuse a saved session
 * - waiting for manual login when needed
 * - saving storageState
 *
 * Receipts listing and downloading are handled by dedicated functions below.
 */

const DEFAULTS = {
  // Publicly stable "iseteenindus/ostuajalugu" URL is not guaranteed.
  // Keep URL configurable and lock it later from real account data.
  receiptsUrl: process.env.MAXIMA_RECEIPTS_URL || 'https://aitah.maxima.ee/ru',
  loggedInSelector: process.env.MAXIMA_LOGGED_IN_SELECTOR || null,
  loggedInUrlRegex: process.env.MAXIMA_LOGGED_IN_URL_REGEX
    ? new RegExp(process.env.MAXIMA_LOGGED_IN_URL_REGEX)
    : null,
  loginWaitMs: Number(process.env.MAXIMA_LOGIN_WAIT_MS || 180000), // 3 minutes
  betweenDownloadsMs: Number(process.env.MAXIMA_BETWEEN_DOWNLOADS_MS || 400),
  maxRetries: Number(process.env.MAXIMA_MAX_RETRIES || 3)
};

const RU_LABELS = {
  myPurchases: '\u043c\u043e\u0438 \u043f\u043e\u043a\u0443\u043f\u043a\u0438',
  nextPage: '\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u043d\u0430 \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0443\u044e \u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0443',
  months: [
    '\u042f\u043d\u0432\u0430\u0440\u044c',
    '\u0424\u0435\u0432\u0440\u0430\u043b\u044c',
    '\u041c\u0430\u0440\u0442',
    '\u0410\u043f\u0440\u0435\u043b\u044c',
    '\u041c\u0430\u0439',
    '\u0418\u044e\u043d\u044c',
    '\u0418\u044e\u043b\u044c',
    '\u0410\u0432\u0433\u0443\u0441\u0442',
    '\u0421\u0435\u043d\u0442\u044f\u0431\u0440\u044c',
    '\u041e\u043a\u0442\u044f\u0431\u0440\u044c',
    '\u041d\u043e\u044f\u0431\u0440\u044c',
    '\u0414\u0435\u043a\u0430\u0431\u0440\u044c'
  ]
};

async function runMaxima({
  from,
  to,
  outDir,
  headless,
  slowMo,
  debug,
  showBrowserAllSteps
}) {
  const authDir = path.resolve(process.cwd(), '.auth');
  const storagePath = path.join(authDir, 'maxima.storage.json');
  await ensureDir(authDir);

  if (showBrowserAllSteps) {
    log.warn('[phase] Legacy mode enabled: showing browser during all automation steps.');
    await runSingleVisiblePhase({ from, to, outDir, storagePath, headless, slowMo, debug });
    return;
  }

  if (headless) {
    log.info('[phase] Ignoring --headless in two-phase mode: login still requires a visible window.');
  }

  await runInteractiveLoginPhase({ storagePath, slowMo, debug });
  await runBackgroundDownloadPhase({ from, to, outDir, storagePath, slowMo, debug });
}

async function runInteractiveLoginPhase({ storagePath, slowMo, debug }) {
  log.info('[phase] Log in in the visible browser...');
  const browser = await chromium.launch({
    headless: false,
    slowMo: slowMo > 0 ? slowMo : undefined
  });
  const context = await chromiumContextWithOptionalState(browser, storagePath);
  const page = await context.newPage();
  configurePageDebug(page, debug);

  try {
    await ensureLoggedInMaxima({ page, context, storagePath, debug });
    log.info('[phase] Login successful. Continuing in background...');
  } catch (e) {
    throw new Error(`[phase1] ${e && e.message ? e.message : String(e)}`);
  } finally {
    await context.close();
    await browser.close();
  }
}

const HEADLESS_SESSION_WAIT_MS = Number(process.env.MAXIMA_HEADLESS_SESSION_WAIT_MS || 30000);

/**
 * Poll until login UI is recognized. Headless may lag behind saved storageState.
 */
async function waitForSessionRecognition(page, maxWaitMs = HEADLESS_SESSION_WAIT_MS) {
  const deadline = Date.now() + maxWaitMs;
  while (Date.now() < deadline) {
    if (await isLoggedInHeuristic(page)) return true;
    await sleep(500);
  }
  return false;
}

async function runBackgroundDownloadPhase({ from, to, outDir, storagePath, slowMo, debug }) {
  log.info('[phase] Processing receipts in background...');
  const browser = await chromium.launch({ headless: true });
  const context = await chromiumContextWithOptionalState(browser, storagePath);
  const page = await context.newPage();
  configurePageDebug(page, debug);
  let usedVisibleFallback = false;

  try {
    await page.goto(DEFAULTS.receiptsUrl, { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle').catch(() => {});

    let loggedIn = await waitForSessionRecognition(page);

    if (!loggedIn) {
      log.warn('[phase] Headless: session not confirmed immediately; reloading page.');
      await page.reload({ waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle').catch(() => {});
      loggedIn = await waitForSessionRecognition(page, Math.min(15000, HEADLESS_SESSION_WAIT_MS));
    }

    if (!loggedIn) {
      log.warn(
        '[phase] Headless could not confirm login state (site behavior may differ in background mode). ' +
          'Switching to visible browser for download.'
      );
      await context.close();
      await browser.close();
      usedVisibleFallback = true;
      await runSingleVisiblePhase({ from, to, outDir, storagePath, headless: false, slowMo, debug });
      return;
    }

    const providerDir = path.join(outDir, 'Maxima');
    await ensureDir(providerDir);

    const { rows, csvPath } = await withRetries(
      async () =>
        await downloadReceiptsMaxima({
          page,
          fromIso: from,
          toIso: to,
          outDir: providerDir,
          debug
        }),
      { attempts: DEFAULTS.maxRetries, delayMs: 1200 }
    );

    log.info(`Done. Downloaded receipts: ${rows.filter((r) => r.filePath).length}/${rows.length}`);
    log.info(`CSV: ${csvPath}`);
  } catch (e) {
    throw new Error(`[phase2] ${e && e.message ? e.message : String(e)}`);
  } finally {
    if (!usedVisibleFallback) {
      await context.close();
      await browser.close();
    }
  }
}

async function runSingleVisiblePhase({ from, to, outDir, storagePath, headless, slowMo, debug }) {
  const browser = await chromium.launch({
    headless,
    slowMo: slowMo > 0 ? slowMo : undefined
  });
  const context = await chromiumContextWithOptionalState(browser, storagePath);
  const page = await context.newPage();
  configurePageDebug(page, debug);

  try {
    await ensureLoggedInMaxima({ page, context, storagePath, debug });
    const providerDir = path.join(outDir, 'Maxima');
    await ensureDir(providerDir);

    const { rows, csvPath } = await withRetries(
      async () =>
        await downloadReceiptsMaxima({
          page,
          fromIso: from,
          toIso: to,
          outDir: providerDir,
          debug
        }),
      { attempts: DEFAULTS.maxRetries, delayMs: 1200 }
    );
    log.info(`Done. Downloaded receipts: ${rows.filter((r) => r.filePath).length}/${rows.length}`);
    log.info(`CSV: ${csvPath}`);
  } finally {
    await context.close();
    await browser.close();
  }
}

function configurePageDebug(page, debug) {
  page.setDefaultTimeout(30000);
  page.setDefaultNavigationTimeout(60000);
  if (!debug) return;
  page.on('console', (msg) => log.info(`[browser console] ${msg.type()}: ${msg.text()}`));
  page.on('pageerror', (err) => log.warn(`[pageerror] ${String(err)}`));
  page.on('requestfailed', (req) =>
    log.warn(`[requestfailed] ${req.url()} ${req.failure() ? req.failure().errorText : ''}`)
  );
}

async function chromiumContextWithOptionalState(browser, storagePath) {
  try {
    const stat = await fs.stat(storagePath);
    if (stat.size > 0) {
      log.info(`Using saved session: ${storagePath}`);
      return await browser.newContext({ storageState: storagePath, acceptDownloads: true });
    }
  } catch (_) {
    // ignore
  }
  return await browser.newContext({ acceptDownloads: true });
}

async function ensureLoggedInMaxima({ page, context, storagePath, debug }) {
  log.info(`Opening receipts/purchase history page: ${DEFAULTS.receiptsUrl}`);
  await page.goto(DEFAULTS.receiptsUrl, { waitUntil: 'domcontentloaded' });

  if (await isLoggedInHeuristic(page)) {
    log.info('You appear to be already logged in (heuristic check).');
    await context.storageState({ path: storagePath });
    return;
  }

  log.warn('No valid session detected or you are logged out.');
  log.info('Please complete login manually (Smart-ID) in the open browser window.');

  const deadline = Date.now() + DEFAULTS.loginWaitMs;
  let lastUrl = '';
  while (Date.now() < deadline) {
    const url = page.url();
    if (url !== lastUrl) {
      log.info(`Current URL: ${url}`);
      lastUrl = url;
    }

    if (await isLoggedInHeuristic(page)) {
      log.info('Login confirmed, saving session.');
      await context.storageState({ path: storagePath });
      return;
    }

    await sleep(1000);
  }

  if (debug) {
    const artifactsDir = path.resolve(process.cwd(), '.artifacts');
    await ensureDir(artifactsDir);
    const p = path.join(artifactsDir, `maxima-login-timeout-${Date.now()}.png`);
    await page.screenshot({ path: p, fullPage: true });
    log.warn(`Screenshot saved: ${p}`);
  }

  throw new Error(
    `Did not detect a successful login within ${Math.round(DEFAULTS.loginWaitMs / 1000)}s. ` +
    `If Smart-ID takes longer, increase MAXIMA_LOGIN_WAIT_MS.`
  );
}

async function isLoggedInHeuristic(page) {
  // 0) Most reliable signal for this flow: "My purchases" page with receipts grid.
  // This state is after login and is the exact page used for downloads.
  const purchasesTitle = page.getByRole('heading', {
    name: new RegExp(`${RU_LABELS.myPurchases}|minu ostud|my purchases`, 'i')
  });
  if (await purchasesTitle.first().isVisible().catch(() => false)) return true;
  const purchasesGrid = page.locator('.MuiDataGrid-root[role="grid"]');
  if (await purchasesGrid.first().isVisible().catch(() => false)) return true;

  // 1) Explicit selector if provided through env.
  if (DEFAULTS.loggedInSelector) {
    const el = page.locator(DEFAULTS.loggedInSelector);
    if (await el.first().isVisible().catch(() => false)) return true;
  }

  // 2) URL check if regex is provided (e.g. /iseteenindus|account|profile/).
  if (DEFAULTS.loggedInUrlRegex && DEFAULTS.loggedInUrlRegex.test(page.url())) {
    return true;
  }

  // 3) Broad heuristic: visible logout link/button.
  const maybeLogout = page.getByRole('link', { name: /logi välja|välju|logout|log out/i });
  if (await maybeLogout.first().isVisible().catch(() => false)) return true;

  // 4) Reverse heuristic: explicit login page control is visible.
  const maybeLogin = page.getByRole('button', { name: /logi sisse|login|sign in/i });
  if (await maybeLogin.first().isVisible().catch(() => false)) return false;

  // If no signals were found, treat as logged out to avoid skipping login.
  return false;
}

async function downloadReceiptsMaxima({ page, fromIso, toIso, outDir, debug }) {
  await ensureOnPurchasesPage(page);
  await waitForPurchasesGrid(page);

  const beforeFirst = await readFirstRowPaymentDate(page);
  await setDateRange(page, fromIso, toIso, { debug });
  await waitForPurchasesGrid(page);
  await waitForGridToMatchRange(page, fromIso, toIso, beforeFirst);

  const allRows = [];
  const seenKeys = new Set();
  const csvRows = [];
  let failedDownloads = 0;
  let downloadIndex = 0;

  for (;;) {
    const pageRows = await readCurrentGridRows(page);
    const uniquePageRows = [];
    for (const r of pageRows) {
      const key = `${r.paymentDateRaw}__${r.captionRaw}__${r.sumRaw}`;
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      allRows.push(r);
      uniquePageRows.push(r);
    }

    for (const r of uniquePageRows) {
      downloadIndex += 1;
      const prefix = `${downloadIndex}`;
      log.info(`${prefix} Downloading receipt: ${r.paymentDateRaw} | ${r.captionRaw} | ${r.sumRaw}`);

      try {
        const res = await withRetries(
          async () => {
            await ensureOnPurchasesPage(page);
            await waitForPurchasesGrid(page);
            return await downloadReceiptForRow(page, r, outDir, debug);
          },
          { attempts: DEFAULTS.maxRetries, delayMs: 800 }
        );
        r.filePath = res.filePath;
        r.suggestedFilename = res.suggestedFilename;
      } catch (e) {
        failedDownloads += 1;
        r.filePath = '';
        r.suggestedFilename = '';
        log.warn(
          `[download] Failed after ${DEFAULTS.maxRetries} attempts: ` +
            `${r.paymentDateRaw} | ${r.captionRaw} | ${r.sumRaw} :: ${e && e.message ? e.message : String(e)}`
        );
      }

      csvRows.push({
        paymentDate: r.paymentDateRaw,
        caption: r.captionRaw,
        sum: r.sumRaw,
        discount: r.discountRaw,
        maximaMoney: r.maximaMoneyRaw,
        filePath: r.filePath || ''
      });

      await sleep(DEFAULTS.betweenDownloadsMs);
    }

    const nextBtn = page.getByRole('button', {
      name: new RegExp(`${RU_LABELS.nextPage}|next page|jargmisele lehele|järgmisele lehele`, 'i')
    });
    const disabled = await nextBtn.isDisabled().catch(() => true);
    if (disabled) break;

    await nextBtn.click();
    await sleep(250);
    await waitForPurchasesGrid(page);
  }

  log.info(`Found unique receipts in table: ${allRows.length}`);
  if (failedDownloads > 0) {
    log.warn(`[download] Failed receipts: ${failedDownloads}`);
  }

  const csvPath = path.join(outDir, 'purchases.csv');
  await fs.writeFile(csvPath, toCsv(csvRows), 'utf8');
  return { rows: allRows, csvPath };
}

async function ensureOnPurchasesPage(page) {
  // If navigation changed or session was lost, return to purchases page.
  const url = page.url() || '';
  if (url.startsWith(DEFAULTS.receiptsUrl)) return;
  await page.goto(DEFAULTS.receiptsUrl, { waitUntil: 'domcontentloaded' });
}

async function waitForPurchasesGrid(page) {
  const grid = page.locator('.MuiDataGrid-root[role="grid"]');
  await grid.first().waitFor({ state: 'visible', timeout: 60000 });
}

async function waitForGridToMatchRange(page, fromIso, toIso, prevFirstPaymentDateRaw) {
  const from = parseIsoDate(fromIso);
  const to = parseIsoDate(toIso);
  const toEnd = new Date(to.getFullYear(), to.getMonth(), to.getDate(), 23, 59, 59, 999);

  const deadline = Date.now() + 30000;
  let lastSeen = '';
  while (Date.now() < deadline) {
    await page.waitForLoadState('networkidle').catch(() => {});

    const cur = await readFirstRowPaymentDate(page);
    if (cur && cur !== lastSeen) {
      log.info(`First row after period change: ${cur}`);
      lastSeen = cur;
    }

    // Validate first row date against requested range when grid is not empty.
    const dt = cur ? parsePaymentDateCell(cur) : null;
    const inRange = dt ? (dt >= from && dt <= toEnd) : false;

    if (inRange) return;
    await sleep(500);
  }

  throw new Error(
    'DateRangePicker period changed, but purchases table did not refresh within 30s. ' +
    'Most likely, the site did not apply the filter immediately.'
  );
}

async function setDateRange(page, fromIso, toIso, { debug } = {}) {
  const from = parseIsoDate(fromIso);
  const to = parseIsoDate(toIso);
  const fromStr = formatEeDate(from);
  const toStr = formatEeDate(to);
  const value = `${fromStr} – ${toStr}`;

  // Preferred method: pick range by clicking Mantine calendar controls.
  // Direct readonly input assignment often does not trigger data refresh.
  try {
    await setDateRangeByClicks(page, from, to);
    return;
  } catch (e) {
    log.warn(`Could not pick dates by clicking calendar, trying input fallback: ${e && e.message ? e.message : String(e)}`);
    if (debug) {
      const artifactsDir = path.resolve(process.cwd(), '.artifacts');
      await ensureDir(artifactsDir);
      const p = path.join(artifactsDir, `maxima-date-click-failed-${Date.now()}.png`);
      await page.screenshot({ path: p, fullPage: true });
      log.warn(`Screenshot saved: ${p}`);
    }
  }

  // Fallback: attempt direct value assignment.
  const input = page.locator('input[name="date"]');
  await input.first().waitFor({ state: 'attached', timeout: 30000 });
  await page.evaluate(({ v }) => {
    const el = document.querySelector('input[name="date"]');
    if (!el) return;
    el.removeAttribute('readonly');
    el.value = v;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
    el.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true }));
    el.setAttribute('readonly', '');
  }, { v: value });
  await sleep(600);
}

async function setDateRangeByClicks(page, fromDate, toDate) {
  const input = page.locator('input[name="date"]');
  await input.first().waitFor({ state: 'visible', timeout: 30000 });
  await openDateRangePicker(page, input.first());

  const calendar = page.locator('.mantine-DateRangePicker-dropdown').first();
  await calendar.waitFor({ state: 'visible', timeout: 10000 });

  // Navigate calendar to start month.
  await goToMonth(calendar, fromDate);
  await clickDayInCurrentMonth(calendar, fromDate.getDate());

  // Navigate to end month and click end date.
  await goToMonth(calendar, toDate);
  await clickDayInCurrentMonth(calendar, toDate.getDate());

  // Close calendar (it may auto-close after range selection).
  await page.keyboard.press('Escape').catch(() => {});
  await sleep(200);
}

async function openDateRangePicker(page, input) {
  await dismissBlockingOverlays(page);
  try {
    await input.click({ timeout: 5000 });
  } catch (_) {
    await dismissBlockingOverlays(page);
    await input.click({ timeout: 5000, force: true });
  }

  const wrapper = page.locator('.mantine-DateRangePicker-dropdownWrapper');
  if (!(await wrapper.first().isVisible().catch(() => false))) {
    await input.focus().catch(() => {});
    await page.keyboard.press('Enter').catch(() => {});
  }
  await wrapper.first().waitFor({ state: 'visible', timeout: 10000 });
}

async function dismissBlockingOverlays(page) {
  const overlays = page.locator('.mantine-Modal-overlay');
  const count = await overlays.count().catch(() => 0);
  if (!count) return;

  let hasVisibleOverlay = false;
  for (let i = 0; i < count; i++) {
    if (await overlays.nth(i).isVisible().catch(() => false)) {
      hasVisibleOverlay = true;
      break;
    }
  }
  if (!hasVisibleOverlay) return;

  log.warn('[date-picker] Modal overlay detected, trying to close it before date selection.');
  await page.keyboard.press('Escape').catch(() => {});
  for (let i = 0; i < count; i++) {
    const overlay = overlays.nth(i);
    if (await overlay.isVisible().catch(() => false)) {
      await overlay.click({ force: true }).catch(() => {});
    }
  }
  await sleep(120);
}

async function goToMonth(calendarRoot, date) {
  const targetYear = date.getFullYear();
  const targetMonthIdx = date.getMonth(); // 0..11
  const targetLabelRu = `${RU_LABELS.months[targetMonthIdx]} ${targetYear}`;

  const level = calendarRoot.locator('.mantine-DateRangePicker-calendarHeaderLevel');
  await level.first().waitFor({ state: 'visible', timeout: 10000 });

  // Detect current month/year from header label.
  for (let i = 0; i < 36; i++) {
    const curText = normalizeSpace(await level.first().innerText().catch(() => ''));
    if (curText.includes(targetLabelRu)) return;

    const cur = parseMonthYear(curText);
    const curKey = cur ? (cur.year * 12 + cur.monthIdx) : null;
    const targetKey = targetYear * 12 + targetMonthIdx;

    // Navigation controls: first is previous, last is next.
    const prev = calendarRoot.locator('.mantine-DateRangePicker-calendarHeaderControl').first();
    const next = calendarRoot.locator('.mantine-DateRangePicker-calendarHeaderControl').last();

    if (curKey == null) {
      // If label parse fails, keep moving forward until match.
      await next.click();
    } else if (curKey < targetKey) {
      await next.click();
    } else {
      await prev.click();
    }
    await sleep(150);
  }

  throw new Error(`Could not navigate to month index ${targetMonthIdx + 1}/${targetYear}`);
}

function parseMonthYear(text) {
  const m = /([\p{L}]+)\s+(\d{4})/u.exec(text || '');
  if (!m) return null;
  const monthName = m[1].toLowerCase();
  const year = Number(m[2]);
  const map = new Map([
    // Russian
    ['\u044f\u043d\u0432\u0430\u0440\u044c', 0],
    ['\u0444\u0435\u0432\u0440\u0430\u043b\u044c', 1],
    ['\u043c\u0430\u0440\u0442', 2],
    ['\u0430\u043f\u0440\u0435\u043b\u044c', 3],
    ['\u043c\u0430\u0439', 4],
    ['\u0438\u044e\u043d\u044c', 5],
    ['\u0438\u044e\u043b\u044c', 6],
    ['\u0430\u0432\u0433\u0443\u0441\u0442', 7],
    ['\u0441\u0435\u043d\u0442\u044f\u0431\u0440\u044c', 8],
    ['\u043e\u043a\u0442\u044f\u0431\u0440\u044c', 9],
    ['\u043d\u043e\u044f\u0431\u0440\u044c', 10],
    ['\u0434\u0435\u043a\u0430\u0431\u0440\u044c', 11],
    // Estonian
    ['jaanuar', 0],
    ['veebruar', 1],
    ['marts', 2],
    ['märts', 2],
    ['aprill', 3],
    ['mai', 4],
    ['juuni', 5],
    ['juuli', 6],
    ['august', 7],
    ['september', 8],
    ['oktoober', 9],
    ['november', 10],
    ['detsember', 11],
    // English
    ['january', 0],
    ['february', 1],
    ['march', 2],
    ['april', 3],
    ['may', 4],
    ['june', 5],
    ['july', 6],
    ['august', 7],
    ['september', 8],
    ['october', 9],
    ['november', 10],
    ['december', 11]
  ]);
  if (!map.has(monthName)) return null;
  return { year, monthIdx: map.get(monthName) };
}

async function clickDayInCurrentMonth(page, dayNumber) {
  // In Mantine, "outside month" days have class mantine-DateRangePicker-outside.
  // Restrict to current month via :not(...) to avoid wrong-day clicks.
  const exact = new RegExp(`^${dayNumber}$`);
  const day = page
    .locator('button.mantine-DateRangePicker-day:not(.mantine-DateRangePicker-outside)')
    .filter({ hasText: exact });

  if (await day.first().isVisible().catch(() => false)) {
    await day.first().click({ force: true });
    await sleep(120);
    return;
  }
  throw new Error(`Could not find day ${dayNumber} in the current month`);
}

async function readFirstRowPaymentDate(page) {
  const zone = page.locator('.MuiDataGrid-virtualScrollerRenderZone');
  const firstRow = zone.locator('.MuiDataGrid-row[data-rowindex]').first();
  const cell = firstRow.locator('[data-field="paymentDate"] .MuiDataGrid-cellContent');
  const t = await cell.first().innerText().catch(() => '');
  return normalizeSpace(t);
}

async function readCurrentGridRows(page) {
  const zone = page.locator('.MuiDataGrid-virtualScrollerRenderZone');
  await zone.first().waitFor({ state: 'visible', timeout: 60000 });

  const rows = zone.locator('.MuiDataGrid-row[data-rowindex]');
  const count = await rows.count();
  const out = [];

  for (let i = 0; i < count; i++) {
    const row = rows.nth(i);
    const paymentDateRaw = await cellText(row, 'paymentDate');
    const captionRaw = await cellText(row, 'caption');
    const sumRaw = await cellText(row, 'sumCents');
    const discountRaw = await cellText(row, 'discountCents');
    const maximaMoneyRaw = await cellText(row, 'maximaMoneyCents');

    if (!paymentDateRaw && !captionRaw) continue;

    out.push({
      paymentDateRaw,
      captionRaw,
      sumRaw,
      discountRaw,
      maximaMoneyRaw
    });
  }
  return out;
}

async function downloadReceiptForRow(page, rowInfo, outDir, debug) {
  // Find row by unique combination (date + store caption + sum).
  const zone = page.locator('.MuiDataGrid-virtualScrollerRenderZone');
  const rows = zone.locator('.MuiDataGrid-row[data-rowindex]');
  const count = await rows.count();

  let targetRow = null;
  for (let i = 0; i < count; i++) {
    const row = rows.nth(i);
    const paymentDateRaw = await cellText(row, 'paymentDate');
    const captionRaw = await cellText(row, 'caption');
    const sumRaw = await cellText(row, 'sumCents');
    if (
      paymentDateRaw === rowInfo.paymentDateRaw &&
      captionRaw === rowInfo.captionRaw &&
      sumRaw === rowInfo.sumRaw
    ) {
      targetRow = row;
      break;
    }
  }

  if (!targetRow) {
    // Row may be on another page; let retries/pagination handle it.
    throw new Error('Could not find row in the current table (possibly on another page).');
  }

  const btn = targetRow.locator('[data-field="receiptDownload"] button');
  await btn.first().waitFor({ state: 'visible', timeout: 30000 });

  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: 60000 }),
    btn.first().click()
  ]);

  const suggested = download.suggestedFilename();

  const paymentDate = parsePaymentDateCell(rowInfo.paymentDateRaw);
  const monthDir = path.join(outDir, yyyyMm(paymentDate));
  await ensureDir(monthDir);

  const safeCaption = slugify(rowInfo.captionRaw).slice(0, 60);
  const safeSum = slugify(rowInfo.sumRaw).slice(0, 20);
  const safeDate = formatFileDateTime(paymentDate);
  const ext = path.extname(suggested || '') || '.pdf';

  const filePath = path.join(monthDir, `${safeDate}__${safeCaption}__${safeSum}${ext}`);

  await download.saveAs(filePath);
  return { filePath, suggestedFilename: suggested };
}

async function cellText(rowLocator, dataField) {
  const cell = rowLocator.locator(`[data-field="${dataField}"] .MuiDataGrid-cellContent`);
  const t = await cell.first().innerText().catch(() => '');
  return normalizeSpace(t);
}

function normalizeSpace(s) {
  return String(s || '').replace(/\s+/g, ' ').trim();
}

function parseIsoDate(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso);
  if (!m) throw new Error(`Invalid ISO date: ${iso}`);
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  const dt = new Date(y, mo - 1, d);
  if (dt.getFullYear() !== y || dt.getMonth() !== mo - 1 || dt.getDate() !== d) {
    throw new Error(`Non-existent date: ${iso}`);
  }
  return dt;
}

function formatEeDate(date) {
  const dd = String(date.getDate()).padStart(2, '0');
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const yy = String(date.getFullYear());
  return `${dd}.${mm}.${yy}`;
}

function parsePaymentDateCell(s) {
  // Example: "23.03.26, 16:08"
  const m = /^(\d{2})\.(\d{2})\.(\d{2}),\s*(\d{2}):(\d{2})$/.exec(normalizeSpace(s));
  if (!m) return new Date();
  const dd = Number(m[1]);
  const mm = Number(m[2]);
  const yy = Number(m[3]);
  const hh = Number(m[4]);
  const min = Number(m[5]);
  const year = 2000 + yy;
  return new Date(year, mm - 1, dd, hh, min);
}

function formatFileDateTime(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const mm = String(date.getMinutes()).padStart(2, '0');
  return `${y}-${m}-${d}T${hh}${mm}`;
}

function slugify(s) {
  return normalizeSpace(s)
    .replace(/\u00A0/g, ' ')
    .replace(/[€]/g, 'EUR')
    .replace(/[^\p{L}\p{N}._-]+/gu, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');
}

module.exports = { runMaxima };

