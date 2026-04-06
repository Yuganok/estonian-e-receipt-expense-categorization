const fs = require('fs/promises');
const path = require('path');

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function writeTextFile(filePath, content) {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, content, 'utf8');
}

module.exports = { ensureDir, writeTextFile };

