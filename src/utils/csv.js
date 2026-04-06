function escapeCsvCell(v) {
  const s = String(v == null ? '' : v);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows) {
  if (!rows || rows.length === 0) return '';
  const headers = Object.keys(rows[0]);
  const lines = [];
  lines.push(headers.map(escapeCsvCell).join(','));
  for (const r of rows) {
    lines.push(headers.map((h) => escapeCsvCell(r[h])).join(','));
  }
  return lines.join('\r\n') + '\r\n';
}

module.exports = { toCsv };

