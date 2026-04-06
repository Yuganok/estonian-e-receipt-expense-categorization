const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  listBankCsvFiles: () => ipcRenderer.invoke('list-bank-csv-files'),
  getAnalysisSummary: (payload) => ipcRenderer.invoke('get-analysis-summary', payload),
  getResearchData: (payload) => ipcRenderer.invoke('get-research-data', payload),
  saveManualCorrections: (payload) => ipcRenderer.invoke('save-manual-corrections', payload),
  exportResearchReport: (payload) => ipcRenderer.invoke('export-research-report', payload),
  startDownload: (opts) => ipcRenderer.send('job-download', opts),
  startPipeline: (opts) => ipcRenderer.send('job-pipeline', opts),
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
