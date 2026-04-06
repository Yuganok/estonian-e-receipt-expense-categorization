function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    const isFlag = next == null || next.startsWith('--');
    out[key] = isFlag ? true : next;
    if (!isFlag) i++;
  }
  // aliases
  if (out.h) out.help = true;
  return out;
}

module.exports = { parseArgs };

