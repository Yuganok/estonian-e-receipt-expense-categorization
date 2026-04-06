const { sleep } = require('./time');
const { log } = require('./log');

async function withRetries(fn, { attempts = 3, delayMs = 500 } = {}) {
  let lastErr = null;
  for (let i = 1; i <= attempts; i++) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      if (i < attempts) {
        log.warn(`Retry ${i}/${attempts} after error: ${e && e.message ? e.message : String(e)}`);
        await sleep(delayMs * i);
        continue;
      }
    }
  }
  throw lastErr;
}

module.exports = { withRetries };

