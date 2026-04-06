function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function yyyyMm(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

module.exports = { sleep, yyyyMm };

