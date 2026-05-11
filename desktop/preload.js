const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  listBankCsvFiles: () => ipcRenderer.invoke('list-bank-csv-files'),
  getAnalysisSummary: (payload) => ipcRenderer.invoke('get-analysis-summary', payload),
  getEvaluationComparison: (payload) => ipcRenderer.invoke('get-evaluation-comparison', payload),
  getResearchData: (payload) => ipcRenderer.invoke('get-research-data', payload),
  saveManualCorrections: (payload) => ipcRenderer.invoke('save-manual-corrections', payload),
  exportResearchReport: (payload) => ipcRenderer.invoke('export-research-report', payload),
  pickSingleReceiptPdf: () => ipcRenderer.invoke('pick-single-receipt-pdf'),
  pickBankCsv: () => ipcRenderer.invoke('pick-bank-csv'),
  pickEvaluationGoldCsv: () => ipcRenderer.invoke('pick-evaluation-gold-csv'),
  startDownload: (opts) => ipcRenderer.send('job-download', opts),
  startPipeline: (opts) => ipcRenderer.send('job-pipeline', opts),
  startEvaluation: (opts) => ipcRenderer.send('job-evaluation', opts),
  clearJobListeners: () => {
    ipcRenderer.removeAllListeners('job-log');
    ipcRenderer.removeAllListeners('job-done');
  },
  onJobLog: (callback) => {
    ipcRenderer.on('job-log', (_event, text) => callback(text));
  },
  onceJobDone: (callback) => {
    ipcRenderer.once('job-done', (_event, payload) => callback(payload));
  }
});
