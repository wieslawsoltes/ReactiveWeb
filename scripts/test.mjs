import { build } from 'esbuild';
import { readdir, mkdir, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
const files = (await readdir('test')).filter(x => x.endsWith('.test.ts')).map(x => `test/${x}`);
await mkdir('.test', { recursive: true });
try {
  await build({ entryPoints: files, outdir: '.test', bundle: false, platform: 'node', format: 'esm', target: 'node20' });
  const result = spawnSync(process.execPath, ['--test', ...files.map(x => x.replace('test/', '.test/').replace(/\.ts$/, '.js'))], { stdio: 'inherit' });
  process.exitCode = result.status ?? 1;
} finally { await rm('.test', { recursive: true, force: true }); }
