function ts() {
  return new Date().toISOString();
}

const log = {
  info: (msg) => process.stdout.write(`[${ts()}] INFO  ${msg}\n`),
  warn: (msg) => process.stdout.write(`[${ts()}] WARN  ${msg}\n`),
  error: (msg) => process.stderr.write(`[${ts()}] ERROR ${msg}\n`)
};

module.exports = { log };

