const logEl = document.getElementById('log');
const fromDate = document.getElementById('fromDate');
const toDate = document.getElementById('toDate');
const bankCsvPathInput = document.getElementById('bankCsvPath');
const bankHint = document.getElementById('bankHint');
const btnPickBankCsv = document.getElementById('btnPickBankCsv');
const storeSelect = document.getElementById('storeSelect');
const fullDateFromField = document.getElementById('fullDateFromField');
const fullDateToField = document.getElementById('fullDateToField');
const fullStoreField = document.getElementById('fullStoreField');
const fullBankField = document.getElementById('fullBankField');
const analysisModeSelect = document.getElementById('analysisModeSelect');
const singleReceiptRow = document.getElementById('singleReceiptRow');
const singleReceiptPdfPath = document.getElementById('singleReceiptPdfPath');
const btnPickSingleReceiptPdf = document.getElementById('btnPickSingleReceiptPdf');
const singleReceiptStoreInput = document.getElementById('singleReceiptStoreInput');
const btnDownload = document.getElementById('btnDownload');
const btnPipeline = document.getElementById('btnPipeline');
const btnBoth = document.getElementById('btnBoth');
const btnClear = document.getElementById('btnClear');
const btnToggleLog = document.getElementById('btnToggleLog');
const logBody = document.getElementById('logBody');
const languageSelect = document.getElementById('languageSelect');

const titleApp = document.getElementById('titleApp');
const subtitleApp = document.getElementById('subtitleApp');
const labelLanguage = document.getElementById('labelLanguage');
const headingSettings = document.getElementById('headingSettings');
const labelFromDate = document.getElementById('labelFromDate');
const labelToDate = document.getElementById('labelToDate');
const labelStore = document.getElementById('labelStore');
const labelAnalysisMode = document.getElementById('labelAnalysisMode');
const analysisModeFullOption = document.getElementById('analysisModeFullOption');
const analysisModeSingleOption = document.getElementById('analysisModeSingleOption');
const labelSingleReceiptPdf = document.getElementById('labelSingleReceiptPdf');
const labelSingleReceiptStore = document.getElementById('labelSingleReceiptStore');
const storeMaximaOption = document.getElementById('storeMaximaOption');
const labelBankCsv = document.getElementById('labelBankCsv');
const headingLog = document.getElementById('headingLog');
const headingResults = document.getElementById('headingResults');
const kpiLabelTotalSpend = document.getElementById('kpiLabelTotalSpend');
const kpiLabelReceipts = document.getElementById('kpiLabelReceipts');
const kpiLabelMatched = document.getElementById('kpiLabelMatched');
const kpiLabelTopCategory = document.getElementById('kpiLabelTopCategory');
const chartTitleCategories = document.getElementById('chartTitleCategories');
const chartTitleMatching = document.getElementById('chartTitleMatching');
const headingResearch = document.getElementById('headingResearch');
const headingParsingReview = document.getElementById('headingParsingReview');
const headingCategorizationReview = document.getElementById('headingCategorizationReview');
const headingBaselineComparison = document.getElementById('headingBaselineComparison');
const btnResearchReload = document.getElementById('btnResearchReload');
const btnSaveCorrections = document.getElementById('btnSaveCorrections');
const btnExportResearch = document.getElementById('btnExportResearch');
const parseSearch = document.getElementById('parseSearch');
const parseFilterKind = document.getElementById('parseFilterKind');
const catSearch = document.getElementById('catSearch');
const catFilterKind = document.getElementById('catFilterKind');
const tableParsingBody = document.querySelector('#tableParsing tbody');
const tableCategorizedBody = document.querySelector('#tableCategorized tbody');
const tableBaselineBody = document.querySelector('#tableBaseline tbody');
const kpiMismatchRate = document.getElementById('kpiMismatchRate');
const kpiMixedReceipts = document.getElementById('kpiMixedReceipts');
const kpiUnmatchedReceipts = document.getElementById('kpiUnmatchedReceipts');
const lblMismatchRate = document.getElementById('lblMismatchRate');
const lblMixedReceipts = document.getElementById('lblMixedReceipts');
const lblUnmatchedReceipts = document.getElementById('lblUnmatchedReceipts');
const headingSourceBreakdown = document.getElementById('headingSourceBreakdown');
const sourceBreakdownLegend = document.getElementById('sourceBreakdownLegend');
const tabData = document.getElementById('tabData');
const tabParsing = document.getElementById('tabParsing');
const tabCategorization = document.getElementById('tabCategorization');
const tabBaseline = document.getElementById('tabBaseline');
const pageData = document.getElementById('pageData');
const pageParsing = document.getElementById('pageParsing');
const pageCategorization = document.getElementById('pageCategorization');
const pageBaseline = document.getElementById('pageBaseline');

const resultsEmpty = document.getElementById('resultsEmpty');
const resultsContent = document.getElementById('resultsContent');
const kpiTotalSpend = document.getElementById('kpiTotalSpend');
const kpiReceipts = document.getElementById('kpiReceipts');
const kpiMatched = document.getElementById('kpiMatched');
const kpiTopCategory = document.getElementById('kpiTopCategory');
const categoryLegend = document.getElementById('categoryLegend');
const matchLegend = document.getElementById('matchLegend');
const categoryCanvas = document.getElementById('categoryChart');
const matchCanvas = document.getElementById('matchChart');

const chartPalette = [
  '#4F8CFF',
  '#48CFAE',
  '#F6C14B',
  '#F38BA8',
  '#AB92FA',
  '#7FD3FF',
  '#FF9F6E',
  '#6EDB8F'
];

let busy = false;
let categoryChart = null;
let matchChart = null;
let logExpanded = false;
let lastOutputDir = '';
let currentLang = 'et';
let researchData = null;
let manualCorrectionsMap = new Map();
let currentPage = 'data';

const PAGE_DEFS = {
  data: { tab: tabData, section: pageData },
  parsing: { tab: tabParsing, section: pageParsing },
  categorization: { tab: tabCategorization, section: pageCategorization },
  baseline: { tab: tabBaseline, section: pageBaseline }
};

const CATEGORY_OPTIONS = [
  'Toidukaubad ja alkoholivabad joogid',
  'Alkohol ja tubakas',
  'Majapidamis- ja puhastusvahendid',
  'Majapidamistehnika',
  'Lilled ja kingitused',
  'DEPOSIT',
  'Muu'
];

const UI_STRINGS = {
  et: {
    language: 'Keel',
    subtitle: 'Tšekkide allalaadimine ja analüüs (Node + Python)',
    settings: 'Seaded',
    fromDate: 'Alguskuupäev',
    toDate: 'Lõppkuupäev',
    analysisMode: 'Analüüsi režiim',
    analysisModeFull: 'Täispipeline (pangaväljavõttega)',
    analysisModeSingle: 'Üksik tšekk (ilma pangaväljavõtteta)',
    store: 'Pood (allalaadimine ja analüüs)',
    storeMaxima: 'Maxima',
    singleReceiptPdfLabel: 'Ühe tšeki PDF',
    singleReceiptStoreLabel: 'Poe nimi',
    singleReceiptBrowse: 'Vali PDF',
    singleReceiptPlaceholder: 'Vali loetava tekstikihiga PDF-tšekk',
    bankCsvLabel: 'Pangaväljavõtte CSV (valikuline)',
    pickBankCsv: 'Vali CSV',
    downloadReceipts: 'Laadi tšekid',
    runAnalysis: 'Käivita analüüs',
    bothSteps: 'Mõlemad sammud',
    clearLog: 'Puhasta logi',
    log: 'Logi',
    showLog: 'Näita logi',
    hideLog: 'Peida logi',
    results: 'Tulemused',
    resultsEmpty: 'Käivita analüüs, et näha visualiseeritud tulemusi.',
    resultsLoadError: 'Tulemuste laadimine ebaõnnestus.',
    kpiTotalSpend: 'Kulud kokku',
    kpiReceipts: 'Tšekke kokku',
    kpiMatched: 'Pangaga sobitatud',
    kpiTopCategory: 'Suurim kategooria',
    chartCategories: 'Kulude jaotus kategooriate kaupa',
    chartMatching: 'Sobitamise katvus',
    researchReview: 'Research review',
    parsingReview: 'Parsing review',
    categorizationReview: 'Categorization review',
    baselineComparison: 'Baseline comparison',
    tabData: 'Andmete kogumine',
    tabParsing: 'Parsimise ulevaade',
    tabCategorization: 'Kategoriseerimise ulevaade',
    tabBaseline: 'Vordlus ja raport',
    reloadResearch: 'Reload research data',
    saveCorrections: 'Save corrections',
    exportResearch: 'Export research report',
    mismatchRate: 'Mismatch rate',
    mixedReceipts: 'Mixed receipts',
    unmatchedReceipts: 'Unmatched receipts',
    sourceBreakdown: 'Category source breakdown',
    sourceDeposit: 'Deposit',
    sourceManualMemory: 'Manual memory',
    sourceRuleMatch: 'Rule match',
    sourceFallbackFood: 'Fallback to food',
    sourceUnknown: 'Unknown (non-product)',
    matchLegendMatched: 'Sobitatud',
    matchLegendUnmatched: 'Sobitamata',
    bankCsvPlaceholder: 'Vali pangaväljavõtte CSV (valikuline)',
    bankHintOptional: 'Täisrežiimis on bank CSV valikuline; ilma selleta jäetakse sobitamine vahele.',
    pickBankCsvFailed: 'CSV valimine ebaõnnestus.',
    pickSingleReceiptFailed: 'PDF valimine ebaõnnestus.',
    singleModeNeedsPdf: 'Vali üksik tšeki PDF enne analüüsi käivitamist.'
  },
  en: {
    language: 'Language',
    subtitle: 'Receipt download and analysis (Node + Python)',
    settings: 'Settings',
    fromDate: 'Start date',
    toDate: 'End date',
    analysisMode: 'Analysis mode',
    analysisModeFull: 'Full pipeline (with bank CSV)',
    analysisModeSingle: 'Single receipt (no bank CSV)',
    store: 'Store (download and analysis)',
    storeMaxima: 'Maxima',
    singleReceiptPdfLabel: 'Single receipt PDF',
    singleReceiptStoreLabel: 'Store label',
    singleReceiptBrowse: 'Browse PDF',
    singleReceiptPlaceholder: 'Select a text-layer receipt PDF',
    bankCsvLabel: 'Bank export CSV (optional)',
    pickBankCsv: 'Browse CSV',
    downloadReceipts: 'Download receipts',
    runAnalysis: 'Run analysis',
    bothSteps: 'Both steps',
    clearLog: 'Clear log',
    log: 'Log',
    showLog: 'Show log',
    hideLog: 'Hide log',
    results: 'Results',
    resultsEmpty: 'Run analysis to see visualized results.',
    resultsLoadError: 'Failed to load results.',
    kpiTotalSpend: 'Total spend',
    kpiReceipts: 'Total receipts',
    kpiMatched: 'Matched with bank',
    kpiTopCategory: 'Top category',
    chartCategories: 'Category spend distribution',
    chartMatching: 'Matching coverage',
    researchReview: 'Research review',
    parsingReview: 'Parsing review',
    categorizationReview: 'Categorization review',
    baselineComparison: 'Baseline comparison',
    tabData: 'Data collection',
    tabParsing: 'Parsing review',
    tabCategorization: 'Categorization review',
    tabBaseline: 'Baseline & reporting',
    reloadResearch: 'Reload research data',
    saveCorrections: 'Save corrections',
    exportResearch: 'Export research report',
    mismatchRate: 'Mismatch rate',
    mixedReceipts: 'Mixed receipts',
    unmatchedReceipts: 'Unmatched receipts',
    sourceBreakdown: 'Category source breakdown',
    sourceDeposit: 'Deposit',
    sourceManualMemory: 'Manual memory',
    sourceRuleMatch: 'Rule match',
    sourceFallbackFood: 'Fallback to food',
    sourceUnknown: 'Unknown (non-product)',
    matchLegendMatched: 'Matched',
    matchLegendUnmatched: 'Unmatched',
    bankCsvPlaceholder: 'Select bank CSV file (optional)',
    bankHintOptional: 'Bank CSV is optional in full mode; if omitted, matching is skipped.',
    pickBankCsvFailed: 'Failed to select CSV file.',
    pickSingleReceiptFailed: 'Failed to select PDF file.',
    singleModeNeedsPdf: 'Select a single receipt PDF before running analysis.'
  }
};

currentLang = loadLanguage();

function loadLanguage() {
  const saved = String(localStorage.getItem('uiLanguage') || '').trim().toLowerCase();
  if (saved && UI_STRINGS[saved]) return saved;
  return 'et';
}

function t(key) {
  const langPack = UI_STRINGS[currentLang] || UI_STRINGS.en;
  if (Object.prototype.hasOwnProperty.call(langPack, key)) return langPack[key];
  return UI_STRINGS.en[key] || key;
}

function applyI18n() {
  document.documentElement.lang = currentLang;
  languageSelect.value = currentLang;

  titleApp.textContent = 'E-Receipt Ledger';
  subtitleApp.textContent = t('subtitle');
  labelLanguage.textContent = t('language');
  headingSettings.textContent = t('settings');
  labelFromDate.textContent = t('fromDate');
  labelToDate.textContent = t('toDate');
  labelAnalysisMode.textContent = t('analysisMode');
  analysisModeFullOption.textContent = t('analysisModeFull');
  analysisModeSingleOption.textContent = t('analysisModeSingle');
  labelStore.textContent = t('store');
  if (storeMaximaOption) storeMaximaOption.textContent = t('storeMaxima');
  if (labelSingleReceiptPdf) labelSingleReceiptPdf.textContent = t('singleReceiptPdfLabel');
  if (labelSingleReceiptStore) labelSingleReceiptStore.textContent = t('singleReceiptStoreLabel');
  if (btnPickSingleReceiptPdf) btnPickSingleReceiptPdf.textContent = t('singleReceiptBrowse');
  if (singleReceiptPdfPath) singleReceiptPdfPath.placeholder = t('singleReceiptPlaceholder');
  labelBankCsv.textContent = t('bankCsvLabel');
  if (btnPickBankCsv) btnPickBankCsv.textContent = t('pickBankCsv');
  if (bankCsvPathInput) bankCsvPathInput.placeholder = t('bankCsvPlaceholder');
  if (bankHint) bankHint.textContent = t('bankHintOptional');
  btnDownload.textContent = t('downloadReceipts');
  btnPipeline.textContent = t('runAnalysis');
  btnBoth.textContent = t('bothSteps');
  btnClear.textContent = t('clearLog');
  headingLog.textContent = t('log');
  headingResults.textContent = t('results');
  kpiLabelTotalSpend.textContent = t('kpiTotalSpend');
  kpiLabelReceipts.textContent = t('kpiReceipts');
  kpiLabelMatched.textContent = t('kpiMatched');
  kpiLabelTopCategory.textContent = t('kpiTopCategory');
  chartTitleCategories.textContent = t('chartCategories');
  chartTitleMatching.textContent = t('chartMatching');
  headingResearch.textContent = t('researchReview');
  headingParsingReview.textContent = t('parsingReview');
  headingCategorizationReview.textContent = t('categorizationReview');
  headingBaselineComparison.textContent = t('baselineComparison');
  btnResearchReload.textContent = t('reloadResearch');
  btnSaveCorrections.textContent = t('saveCorrections');
  btnExportResearch.textContent = t('exportResearch');
  lblMismatchRate.textContent = t('mismatchRate');
  lblMixedReceipts.textContent = t('mixedReceipts');
  lblUnmatchedReceipts.textContent = t('unmatchedReceipts');
  if (headingSourceBreakdown) headingSourceBreakdown.textContent = t('sourceBreakdown');
  tabData.textContent = t('tabData');
  tabParsing.textContent = t('tabParsing');
  tabCategorization.textContent = t('tabCategorization');
  tabBaseline.textContent = t('tabBaseline');

  setLogExpanded(logExpanded);
}

function isSingleMode() {
  return String(analysisModeSelect.value || 'full').trim().toLowerCase() === 'single';
}

function updateModeUi() {
  const single = isSingleMode();
  if (singleReceiptRow) singleReceiptRow.classList.toggle('hidden', !single);
  if (fullDateFromField) fullDateFromField.classList.toggle('hidden', single);
  if (fullDateToField) fullDateToField.classList.toggle('hidden', single);
  if (fullStoreField) fullStoreField.classList.toggle('hidden', single);
  if (fullBankField) fullBankField.classList.toggle('hidden', single);
  fromDate.disabled = single || busy;
  toDate.disabled = single || busy;
  storeSelect.disabled = single || busy;
  if (bankCsvPathInput) bankCsvPathInput.disabled = single || busy;
  if (btnPickBankCsv) btnPickBankCsv.disabled = single || busy;
  btnDownload.classList.toggle('hidden', single);
  btnBoth.classList.toggle('hidden', single);
  btnDownload.disabled = busy || single;
  btnBoth.disabled = busy || single;
  updatePipelineButtons();
}

function formatLocalizedValue(key, ...args) {
  const value = t(key);
  if (typeof value === 'function') return value(...args);
  return String(value || '');
}

function loadActivePage() {
  const saved = String(localStorage.getItem('uiActivePage') || '').trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(PAGE_DEFS, saved) ? saved : 'data';
}

function setActivePage(pageKey) {
  const next = Object.prototype.hasOwnProperty.call(PAGE_DEFS, pageKey) ? pageKey : 'data';
  currentPage = next;
  localStorage.setItem('uiActivePage', next);
  Object.entries(PAGE_DEFS).forEach(([key, def]) => {
    const active = key === next;
    def.section.classList.toggle('hidden', !active);
    def.tab.classList.toggle('active', active);
  });
}

function appendLog(text) {
  logEl.textContent += text;
  logEl.scrollTop = logEl.scrollHeight;
}

function setLogExpanded(expanded) {
  logExpanded = Boolean(expanded);
  logBody.classList.toggle('hidden', !logExpanded);
  btnToggleLog.textContent = logExpanded ? t('hideLog') : t('showLog');
}

function setBusy(on) {
  busy = on;
  analysisModeSelect.disabled = on;
  btnPickSingleReceiptPdf.disabled = on;
  singleReceiptStoreInput.disabled = on;
  updatePipelineButtons();
  btnClear.disabled = on;
  updateModeUi();
}

function updatePipelineButtons() {
  const single = isSingleMode();
  const hasSinglePdf = Boolean(String(singleReceiptPdfPath.value || '').trim());
  const block = busy || (single ? !hasSinglePdf : false);
  btnPipeline.disabled = block;
  btnBoth.disabled = single || block;
}

function readForm() {
  return {
    from: fromDate.value.trim(),
    to: toDate.value.trim(),
    bankCsvPath: bankCsvPathInput ? bankCsvPathInput.value.trim() : '',
    stores: 'maxima',
    mode: isSingleMode() ? 'single' : 'full',
    singleReceiptPdfPath: String(singleReceiptPdfPath.value || '').trim(),
    singleReceiptStore: String(singleReceiptStoreInput.value || '').trim() || 'Selver'
  };
}

function bindJobSession() {
  window.electronAPI.clearJobListeners();
  window.electronAPI.onJobLog((text) => appendLog(text));
  return new Promise((resolve) => {
    window.electronAPI.onceJobDone((payload) => resolve(payload));
  });
}

function validateDownload(form) {
  if (!form.from || !form.to) {
    appendLog('Please set both start and end dates.\n');
    return false;
  }
  if (form.from > form.to) {
    appendLog('Start date cannot be later than end date.\n');
    return false;
  }
  return true;
}

function validatePipeline(form) {
  if (form.mode === 'single') {
    if (!form.singleReceiptPdfPath) {
      appendLog(`${t('singleModeNeedsPdf')}\n`);
      return false;
    }
    return true;
  }
  return true;
}

function asEuro(value) {
  return `${Number(value || 0).toFixed(2)} EUR`;
}

function asPct(value) {
  return `${Number(value || 0).toFixed(1)}%`;
}

function destroyCharts() {
  if (categoryChart) {
    categoryChart.destroy();
    categoryChart = null;
  }
  if (matchChart) {
    matchChart.destroy();
    matchChart = null;
  }
}

function renderLegend(container, rows, formatter) {
  container.innerHTML = '';
  for (const row of rows) {
    const item = document.createElement('div');
    item.className = 'legend-item';
    const dot = document.createElement('span');
    dot.className = 'legend-dot';
    dot.style.backgroundColor = row.color;
    const label = document.createElement('span');
    label.className = 'legend-label';
    label.textContent = row.label;
    const value = document.createElement('span');
    value.className = 'legend-value';
    value.textContent = formatter(row);
    item.appendChild(dot);
    item.appendChild(label);
    item.appendChild(value);
    container.appendChild(item);
  }
}

function renderResults(summary) {
  const categories = summary?.charts?.categories || [];
  const coverage = summary?.charts?.matchCoverage || { matched: 0, unmatched: 0 };
  const kpi = summary?.kpi || {};

  kpiTotalSpend.textContent = asEuro(kpi.totalSpendEur);
  kpiReceipts.textContent = `${kpi.receiptsTotal || 0}`;
  kpiMatched.textContent = asPct(kpi.matchedRatePct);
  kpiTopCategory.textContent = kpi.topCategory
    ? `${kpi.topCategory} (${asPct(kpi.topCategoryPct)})`
    : '-';

  const catColors = categories.map((_, idx) => chartPalette[idx % chartPalette.length]);
  const categoryData = categories.map((c) => Number(c.amountEur || 0));
  const categoryLabels = categories.map((c) => c.category);

  categoryChart = new Chart(categoryCanvas, {
    type: 'doughnut',
    data: {
      labels: categoryLabels,
      datasets: [
        {
          data: categoryData,
          backgroundColor: catColors,
          borderWidth: 0
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '62%',
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.label}: ${Number(ctx.raw || 0).toFixed(2)} EUR`
          }
        }
      }
    }
  });

  const matchedColor = '#48CFAE';
  const unmatchedColor = '#445066';
  matchChart = new Chart(matchCanvas, {
    type: 'doughnut',
    data: {
      labels: [t('matchLegendMatched'), t('matchLegendUnmatched')],
      datasets: [
        {
          data: [coverage.matched || 0, coverage.unmatched || 0],
          backgroundColor: [matchedColor, unmatchedColor],
          borderWidth: 0
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '62%',
      plugins: {
        legend: { display: false }
      }
    }
  });

  renderLegend(
    categoryLegend,
    categories.map((c, idx) => ({
      label: c.category,
      value: c.amountEur,
      share: c.sharePct,
      color: catColors[idx]
    })),
    (row) => `${asEuro(row.value)} · ${asPct(row.share)}`
  );

  renderLegend(
    matchLegend,
    [
      { label: t('matchLegendMatched'), value: coverage.matched || 0, color: matchedColor },
      { label: t('matchLegendUnmatched'), value: coverage.unmatched || 0, color: unmatchedColor }
    ],
    (row) => `${row.value}`
  );
}

function correctionKey(row) {
  return `${String(row.receipt_id || '')}__${String(row.item_text || '')}`;
}

function getManualCategory(row) {
  const c = manualCorrectionsMap.get(correctionKey(row));
  return c ? String(c.manual_category || '') : '';
}

function getFinalCategory(row) {
  return getManualCategory(row) || String(row.category || '');
}

function ensureManualCorrection(row, manualCategory) {
  const key = correctionKey(row);
  if (!manualCategory) {
    manualCorrectionsMap.delete(key);
    return;
  }
  manualCorrectionsMap.set(key, {
    receipt_id: String(row.receipt_id || ''),
    item_text: String(row.item_text || ''),
    manual_category: manualCategory,
    note: '',
    updated_at: new Date().toISOString()
  });
}

function renderParsingTable() {
  const rows = researchData?.itemsRaw || [];
  const q = String(parseSearch.value || '').trim().toLowerCase();
  const kind = String(parseFilterKind.value || 'all');
  const filtered = rows.filter((r) => {
    const text = String(r.item_text || '').toLowerCase();
    if (q && !text.includes(q)) return false;
    if (kind === 'noise' && !r.is_noise) return false;
    if (kind === 'deposit' && !r.is_deposit) return false;
    if (kind === 'clean' && (r.is_deposit || r.is_noise)) return false;
    return true;
  });

  tableParsingBody.innerHTML = '';
  for (const r of filtered.slice(0, 400)) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${String(r.store || '')}</td>
      <td>${String(r.receipt_id || '')}</td>
      <td>${String(r.item_text || '')}</td>
      <td>${String(r.gross_price || '')}</td>
      <td>${String(r.discount || '')}</td>
      <td>${r.is_deposit ? 'yes' : 'no'}</td>
      <td>${r.is_noise ? 'yes' : 'no'}</td>
    `;
    tableParsingBody.appendChild(tr);
  }
}

function renderCategoryFilterOptions() {
  const rows = researchData?.itemsCategorized || [];
  const categories = new Set(['all']);
  for (const r of rows) {
    categories.add(String(r.category || ''));
  }
  const prev = catFilterKind.value || 'all';
  catFilterKind.innerHTML = '';
  for (const c of categories) {
    if (!c) continue;
    const opt = document.createElement('option');
    opt.value = c;
    opt.textContent = c === 'all' ? 'All categories' : c;
    catFilterKind.appendChild(opt);
  }
  catFilterKind.value = Array.from(categories).includes(prev) ? prev : 'all';
}

function renderCategorizedTable() {
  const rows = researchData?.itemsCategorized || [];
  const q = String(catSearch.value || '').trim().toLowerCase();
  const cat = String(catFilterKind.value || 'all');
  const hasRuleColumn = rows.some((r) => Object.prototype.hasOwnProperty.call(r, 'category_rule'));
  const filtered = rows.filter((r) => {
    const text = String(r.item_text || '').toLowerCase();
    if (q && !text.includes(q)) return false;
    if (cat !== 'all' && String(getFinalCategory(r)) !== cat && String(r.category || '') !== cat) return false;
    return true;
  });
  tableCategorizedBody.innerHTML = '';
  for (const r of filtered.slice(0, 400)) {
    const tr = document.createElement('tr');
    const tdSelect = document.createElement('td');
    const select = document.createElement('select');
    select.className = 'inline-select';
    const empty = document.createElement('option');
    empty.value = '';
    empty.textContent = `(auto: ${String(r.category || '')})`;
    select.appendChild(empty);
    for (const c of CATEGORY_OPTIONS) {
      const opt = document.createElement('option');
      opt.value = c;
      opt.textContent = c;
      select.appendChild(opt);
    }
    select.value = getManualCategory(r);
    select.addEventListener('change', () => {
      ensureManualCorrection(r, String(select.value || ''));
      renderBaselineSection();
    });
    tdSelect.appendChild(select);

    const rawRule = String(r.category_rule ?? '').trim();
    const ruleDisplay = !hasRuleColumn
      ? '(not available in this session)'
      : (rawRule || '(default)');

    tr.innerHTML = `
      <td>${String(r.store || '')}</td>
      <td>${String(r.receipt_id || '')}</td>
      <td>${String(r.item_text || '')}</td>
      <td>${String(r.category || '')}</td>
      <td>${ruleDisplay}</td>
    `;
    tr.appendChild(tdSelect);
    tableCategorizedBody.appendChild(tr);
  }
}

function renderBaselineSection() {
  const matched = researchData?.matched || [];
  const metrics = researchData?.metrics || { mismatchRatePct: 0, mixedReceipts: 0, receiptsUnmatched: 0 };
  const sourceRows = Array.isArray(researchData?.categorySourceBreakdown)
    ? researchData.categorySourceBreakdown
    : [];
  kpiMismatchRate.textContent = `${Number(metrics.mismatchRatePct || 0).toFixed(1)}%`;
  kpiMixedReceipts.textContent = String(metrics.mixedReceipts || 0);
  kpiUnmatchedReceipts.textContent = String(metrics.receiptsUnmatched || 0);
  renderSourceBreakdown(sourceRows);

  tableBaselineBody.innerHTML = '';
  for (const r of matched.slice(0, 250)) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${String(r.receipt_id || '')}</td>
      <td>${String(r.store || '')}</td>
      <td>${String(r.purchase_date || '')}</td>
      <td>${String(r.receipt_total_eur || '')}</td>
      <td>${String(r.bank_tx_date || '')}</td>
      <td>${String(r.bank_tx_amount || '')}</td>
      <td>${String(r.match_confidence || '')}</td>
    `;
    tableBaselineBody.appendChild(tr);
  }
}

function sourceLabel(source) {
  const key = String(source || '').trim().toLowerCase();
  if (key === 'deposit') return t('sourceDeposit');
  if (key === 'manual_memory') return t('sourceManualMemory');
  if (key === 'rule_match') return t('sourceRuleMatch');
  if (key === 'fallback_food') return t('sourceFallbackFood');
  if (key === 'unknown') return t('sourceUnknown');
  return key || 'unknown';
}

function renderSourceBreakdown(rows) {
  if (!sourceBreakdownLegend) return;
  const sourcePalette = {
    deposit: '#7FD3FF',
    manual_memory: '#AB92FA',
    rule_match: '#48CFAE',
    fallback_food: '#F6C14B',
    unknown: '#F38BA8'
  };
  const normalized = (rows || []).map((row) => {
    const source = String(row.category_source || '').trim().toLowerCase() || 'unknown';
    return {
      source,
      label: sourceLabel(source),
      amountEur: Number(row.amount_eur || 0),
      sharePctSpend: Number(row.share_pct_spend || 0),
      rowCount: Number(row.row_count || 0),
      sharePctRows: Number(row.share_pct_rows || 0),
      color: sourcePalette[source] || '#6EDB8F'
    };
  });
  renderLegend(
    sourceBreakdownLegend,
    normalized,
    (row) =>
      `${row.rowCount} rows (${asPct(row.sharePctRows)}) · ${asEuro(row.amountEur)} (${asPct(
        row.sharePctSpend
      )})`
  );
}

async function refreshResearchData() {
  const payload = lastOutputDir ? { outputDir: lastOutputDir } : undefined;
  const res = await window.electronAPI.getResearchData(payload);
  if (!res || !res.ok) {
    appendLog(`[warn] Could not load research data: ${res && res.error ? res.error : 'unknown'}\n`);
    return;
  }
  researchData = res;
  lastOutputDir = String(res.outputDir || lastOutputDir || '');
  const loadedCorrections = Array.isArray(res.manualCorrections) ? res.manualCorrections : [];
  manualCorrectionsMap = new Map(
    loadedCorrections.map((c) => [`${c.receipt_id}__${c.item_text}`, c])
  );
  renderCategoryFilterOptions();
  renderParsingTable();
  renderCategorizedTable();
  renderBaselineSection();
}

async function saveCorrections() {
  if (!researchData || !researchData.outputDir) {
    appendLog('[warn] Run analysis first to create a session output.\n');
    return;
  }
  const corrections = Array.from(manualCorrectionsMap.values());
  const res = await window.electronAPI.saveManualCorrections({
    outputDir: researchData.outputDir,
    corrections
  });
  if (res && res.ok) {
    appendLog(`[info] Saved manual corrections: ${res.saved}\n`);
  } else {
    appendLog(`[error] Could not save manual corrections: ${res && res.error ? res.error : 'unknown'}\n`);
  }
}

async function exportResearch() {
  if (!researchData || !researchData.outputDir) {
    appendLog('[warn] Run analysis first to export research data.\n');
    return;
  }
  const corrections = Array.from(manualCorrectionsMap.values());
  const res = await window.electronAPI.exportResearchReport({
    outputDir: researchData.outputDir,
    manualCorrections: corrections
  });
  if (res && res.ok) {
    appendLog(`[info] Research report exported: ${res.reportTxtPath}\n`);
  } else {
    appendLog(`[error] Could not export research report: ${res && res.error ? res.error : 'unknown'}\n`);
  }
}

async function refreshResults() {
  let summary;
  try {
    summary = await window.electronAPI.getAnalysisSummary(lastOutputDir ? { outputDir: lastOutputDir } : undefined);
  } catch (e) {
    summary = { ok: false, empty: true, reason: e.message || 'unknown' };
  }
  if (summary && summary.ok && summary.outputDir) {
    lastOutputDir = String(summary.outputDir);
  }

  destroyCharts();

  if (!summary || !summary.ok || summary.empty) {
    resultsContent.classList.add('hidden');
    resultsEmpty.classList.remove('hidden');
    resultsEmpty.textContent =
      summary?.reason === 'error'
        ? t('resultsLoadError')
        : t('resultsEmpty');
    categoryLegend.innerHTML = '';
    matchLegend.innerHTML = '';
    kpiTotalSpend.textContent = '0 EUR';
    kpiReceipts.textContent = '0';
    kpiMatched.textContent = '0%';
    kpiTopCategory.textContent = '-';
    return;
  }

  resultsEmpty.classList.add('hidden');
  resultsContent.classList.remove('hidden');
  renderResults(summary);
}

analysisModeSelect.addEventListener('change', updateModeUi);
tabData.addEventListener('click', () => setActivePage('data'));
tabParsing.addEventListener('click', () => setActivePage('parsing'));
tabCategorization.addEventListener('click', () => setActivePage('categorization'));
tabBaseline.addEventListener('click', () => setActivePage('baseline'));

languageSelect.addEventListener('change', async () => {
  const selected = String(languageSelect.value || 'et').trim().toLowerCase();
  currentLang = UI_STRINGS[selected] ? selected : 'et';
  localStorage.setItem('uiLanguage', currentLang);
  applyI18n();
  await refreshResults();
  await refreshResearchData();
});

if (btnPickBankCsv) {
  btnPickBankCsv.addEventListener('click', async () => {
    try {
      const res = await window.electronAPI.pickBankCsv();
      if (res && res.ok && res.path && bankCsvPathInput) {
        bankCsvPathInput.value = String(res.path);
        updatePipelineButtons();
      }
    } catch (_e) {
      appendLog(`${t('pickBankCsvFailed')}\n`);
    }
  });
}

btnPickSingleReceiptPdf.addEventListener('click', async () => {
  try {
    const res = await window.electronAPI.pickSingleReceiptPdf();
    if (res && res.ok && res.path) {
      singleReceiptPdfPath.value = String(res.path);
      updatePipelineButtons();
    }
  } catch (_e) {
    appendLog(`${t('pickSingleReceiptFailed')}\n`);
  }
});

btnResearchReload.addEventListener('click', () => {
  refreshResearchData();
});

btnSaveCorrections.addEventListener('click', () => {
  saveCorrections();
});

btnExportResearch.addEventListener('click', () => {
  exportResearch();
});

parseSearch.addEventListener('input', () => renderParsingTable());
parseFilterKind.addEventListener('change', () => renderParsingTable());
catSearch.addEventListener('input', () => renderCategorizedTable());
catFilterKind.addEventListener('change', () => renderCategorizedTable());

btnDownload.addEventListener('click', async () => {
  const form = readForm();
  if (!validateDownload(form)) return;
  setBusy(true);
  const done = bindJobSession();
  appendLog('--- Receipt Download ---\n');
  appendLog('Open browser login. After successful confirmation, the process continues in background mode.\n');
  window.electronAPI.startDownload({
    from: form.from,
    to: form.to,
    stores: form.stores
  });
  const result = await done;
  appendLog(result.ok ? '\nDone.\n' : `\nFinished with error code ${result.code}.\n`);
  setBusy(false);
});

btnPipeline.addEventListener('click', async () => {
  const form = readForm();
  if (!validatePipeline(form)) return;
  setBusy(true);
  const done = bindJobSession();
  appendLog('--- Analysis (Python) ---\n');
  window.electronAPI.startPipeline({
    mode: form.mode,
    bankCsvPath: form.bankCsvPath,
    stores: form.stores,
    from: form.from,
    to: form.to,
    singleReceiptPdfPath: form.singleReceiptPdfPath,
    singleReceiptStore: form.singleReceiptStore
  });
  const result = await done;
  if (result && result.outputDir) lastOutputDir = String(result.outputDir);
  appendLog(result.ok ? '\nDone.\n' : `\nFinished with error code ${result.code}.\n`);
  setBusy(false);
  if (result.ok) {
    await refreshResults();
    await refreshResearchData();
  }
});

btnBoth.addEventListener('click', async () => {
  const form = readForm();
  if (!validateDownload(form) || !validatePipeline(form)) return;
  setBusy(true);

  let done = bindJobSession();
  appendLog('--- Step 1: Download ---\n');
  appendLog('Open browser login. After successful confirmation, the process continues in background mode.\n');
  window.electronAPI.startDownload({
    from: form.from,
    to: form.to,
    stores: form.stores
  });
  const step1 = await done;
  appendLog(step1.ok ? '\nStep 1 done.\n' : `\nStep 1 failed with code ${step1.code}.\n`);
  if (!step1.ok) {
    setBusy(false);
    return;
  }

  done = bindJobSession();
  appendLog('\n--- Step 2: Analysis ---\n');
  window.electronAPI.startPipeline({
    mode: 'full',
    bankCsvPath: form.bankCsvPath,
    stores: form.stores,
    from: form.from,
    to: form.to
  });
  const step2 = await done;
  if (step2 && step2.outputDir) lastOutputDir = String(step2.outputDir);
  appendLog(step2.ok ? '\nBoth steps are complete.\n' : `\nStep 2 failed with code ${step2.code}.\n`);
  setBusy(false);
  if (step2.ok) {
    await refreshResults();
    await refreshResearchData();
  }
});

btnClear.addEventListener('click', () => {
  logEl.textContent = '';
});

btnToggleLog.addEventListener('click', () => {
  setLogExpanded(!logExpanded);
});

function defaultDateRange() {
  const today = new Date();
  const y = today.getFullYear();
  const m = String(today.getMonth() + 1).padStart(2, '0');
  const d = String(today.getDate()).padStart(2, '0');
  toDate.value = `${y}-${m}-${d}`;

  const start = new Date(today);
  start.setMonth(start.getMonth() - 1);
  const y0 = start.getFullYear();
  const m0 = String(start.getMonth() + 1).padStart(2, '0');
  const d0 = String(start.getDate()).padStart(2, '0');
  fromDate.value = `${y0}-${m0}-${d0}`;
}

defaultDateRange();
try {
  setActivePage(loadActivePage());
  applyI18n();
  updateModeUi();
  refreshResults();
  refreshResearchData();
} catch (e) {
  appendLog(`Renderer init failed: ${e && e.message ? e.message : String(e)}\n`);
}
