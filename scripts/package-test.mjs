import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { resolve, join, dirname } from 'node:path';
import { spawnSync } from 'node:child_process';

const project = resolve('.');
const packageJson = JSON.parse(await readFile(join(project, 'package.json'), 'utf8'));
const args = process.argv.slice(2);
assert(args.length === 0 || (args.length === 2 && args[0] === '--tarball'), 'Usage: node scripts/package-test.mjs [--tarball path]');
const suppliedTarball = args.length ? resolve(args[1]) : undefined;
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
function run(command, args, cwd, label) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8', timeout: 60_000, env: { ...process.env, npm_config_update_notifier: 'false' } });
  assert.equal(result.status, 0, `${label} failed:\n${result.stdout ?? ''}${result.stderr ?? ''}${result.error ?? ''}`);
  return result.stdout;
}

await mkdir(join(project, 'test-results'), { recursive: true });
const temporary = await mkdtemp(join(project, 'test-results/package-'));
try {
  const packResult = JSON.parse(run(npm, ['pack', ...(suppliedTarball ? [suppliedTarball] : []), '--ignore-scripts', '--json', '--pack-destination', temporary], project, 'npm pack'));
  assert.equal(packResult[0].name, packageJson.name, 'Tarball package name must match the checked-out source');
  assert.equal(packResult[0].version, packageJson.version, 'Tarball version must match the checked-out source');
  const tarball = join(temporary, packResult[0].filename);
  const packedFiles = new Set(packResult[0].files.map(file => file.path));
  for (const file of ['dist/index.js', 'dist/index.d.ts', 'dist/cjs/index.js', 'dist/cjs/package.json', 'dist/generator-cli.js', 'dist/react.js', 'dist/html.js', 'dist/generation.js', 'src/index.ts', 'docs/generation.md', 'examples/generation/counter.schema.json']) {
    assert(packedFiles.has(file), `Published tarball is missing ${file}`);
  }
  for (const file of packedFiles) assert(!file.startsWith('node_modules/') && !file.startsWith('test-results/'), `Unexpected package file: ${file}`);

  const consumer = join(temporary, 'consumer');
  await mkdir(consumer);
  await writeFile(join(consumer, 'package.json'), JSON.stringify({ name: 'reactiveweb-package-smoke', version: '1.0.0', private: true, type: 'module' }));
  run(npm, ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund', '--package-lock=false', '--legacy-peer-deps', tarball], consumer, 'isolated local-tarball installation');
  // Peers remain external and use the caller's installation. No network is needed.
  for (const dependency of ['rxjs', 'react', '@types/node', '@types/react']) {
    const destination = join(consumer, 'node_modules', dependency);
    await mkdir(dirname(destination), { recursive: true });
    await symlink(join(project, 'node_modules', dependency), destination, process.platform === 'win32' ? 'junction' : 'dir');
  }
  const importName = packageJson.name;
  const esm = `
import assert from 'node:assert/strict';
import * as core from ${JSON.stringify(importName)};
import * as html from ${JSON.stringify(importName + '/html')};
import * as react from ${JSON.stringify(importName + '/react')};
import * as generation from ${JSON.stringify(importName + '/generation')};
import * as generator from ${JSON.stringify(importName + '/generator')};
import { Observable, firstValueFrom, map } from 'rxjs';
assert.equal(typeof html.ReactiveElement, 'function');
assert.equal(typeof react.useObservable, 'function');
assert.equal(typeof generator.GenerateViewModelSource, 'function');
assert.equal(core.defineViewModel, generation.defineViewModel);
const Model = generation.defineViewModel({ properties: { Count: generation.reactiveProperty(3) }, computed: { Double: { source: vm => vm.WhenAnyValue('Count').pipe(map(value => value * 2)), initialValue: 0 } }, commands: { Add: { execute: (vm, input) => vm.Count += input } } });
const vm = new Model();
assert(vm instanceof core.ReactiveObject, 'Shared ESM class identity');
assert(vm.Add instanceof core.ReactiveCommand, 'Shared ESM command identity');
assert(vm.Changed instanceof Observable, 'RxJS must remain an external peer');
assert(vm.Add.Execute(2) instanceof Observable, 'Commands return peer RxJS observables');
assert.equal(await firstValueFrom(vm.Add.Execute(2)), 5);
assert.equal(vm.Double, 10);
vm.Dispose();
console.log('ESM package entries, class identity, and external RxJS: passed');
`;
  const commonjs = `
const assert = require('node:assert/strict');
const core = require(${JSON.stringify(importName)});
const html = require(${JSON.stringify(importName + '/html')});
const react = require(${JSON.stringify(importName + '/react')});
const generation = require(${JSON.stringify(importName + '/generation')});
const generator = require(${JSON.stringify(importName + '/generator')});
const { Observable, firstValueFrom } = require('rxjs');
assert.equal(typeof html.ReactiveElement, 'function');
assert.equal(typeof react.useObservable, 'function');
assert.equal(typeof generator.GenerateViewModelSource, 'function');
assert.equal(core.defineViewModel, generation.defineViewModel);
const Model = generation.defineViewModel({ properties: { Count: generation.reactiveProperty(3) }, commands: { Add: { execute: (vm, input) => vm.Count += input } } });
const vm = new Model();
assert(vm instanceof core.ReactiveObject, 'Shared CommonJS class identity across package entry points');
assert(vm.Add instanceof core.ReactiveCommand, 'Shared CommonJS command identity across package entry points');
assert(vm.Changed instanceof Observable, 'CommonJS must share caller RxJS');
firstValueFrom(vm.Add.Execute(2)).then(value => { assert.equal(value, 5); vm.Dispose(); console.log('CommonJS package entries, class identity, and external RxJS: passed'); });
`;
  await writeFile(join(consumer, 'esm.mjs'), esm);
  await writeFile(join(consumer, 'commonjs.cjs'), commonjs);
  process.stdout.write(run(process.execPath, ['esm.mjs'], consumer, 'ESM package consumer'));
  process.stdout.write(run(process.execPath, ['commonjs.cjs'], consumer, 'CommonJS package consumer'));

  const typed = `
import { ReactiveObject, ReactiveCommand, Reactive } from ${JSON.stringify(importName)};
import { defineViewModel, reactiveProperty } from ${JSON.stringify(importName + '/generation')};
import { ReactiveElement } from ${JSON.stringify(importName + '/html')};
import { useObservable } from ${JSON.stringify(importName + '/react')};
import { GenerateViewModelSource } from ${JSON.stringify(importName + '/generator')};
import { firstValueFrom, map } from 'rxjs';
class Person extends ReactiveObject { @Reactive accessor Name = 'Ada'; }
class PersonView extends ReactiveElement<Person> {}
const Model = defineViewModel({ properties: { Count: reactiveProperty(1) }, computed: { Double: { source: vm => vm.WhenAnyValue<number>('Count').pipe(map(value => value * 2)), initialValue: 0 } }, commands: { Add: { execute: (vm, input: number) => vm.Count += input } } });
const model = new Model({ Count: 2 });
const count: number = model.Count;
const twice: number = model.Double;
const command: ReactiveCommand<number, number> = model.Add;
const result: Promise<number> = firstValueFrom(command.Execute(1));
const generated: string = GenerateViewModelSource({ className: 'Example', properties: { Name: { initial: 'Ada', type: 'string' } } });
void [count, twice, command, result, generated, PersonView, useObservable];
// @ts-expect-error Strong property types must reject invalid assignments.
model.Count = 'invalid';
// @ts-expect-error Command inputs retain their declared type.
model.Add.Execute('invalid');
`;
  await writeFile(join(consumer, 'consumer.ts'), typed);
  await writeFile(join(consumer, 'consumer.cts'), `import core = require(${JSON.stringify(importName)});\nimport generation = require(${JSON.stringify(importName + '/generation')});\nconst model = new (generation.defineViewModel({ properties: { Count: generation.reactiveProperty(1) } }))();\nconst count: number = model.Count;\nconst object: core.ReactiveObject = model;\nvoid [count, object];\n`);
  run(process.execPath, [join(project, 'node_modules/typescript/bin/tsc'), '--ignoreConfig', '--noEmit', '--strict', '--target', 'ES2022', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--skipLibCheck', '--lib', 'ES2022,DOM,ESNext.Decorators', 'consumer.ts', 'consumer.cts'], consumer, 'strict TypeScript ESM/CommonJS consumers');
  console.log('Strict TypeScript consumers and negative type assertions: passed');

  await writeFile(join(consumer, 'counter.schema.json'), JSON.stringify({ className: 'Counter', properties: { Count: { type: 'number', initial: 5, validate: { minimum: 0 } } } }));
  const installedBin = join(consumer, 'node_modules/.bin/reactiveweb-generate');
  run(process.execPath, [installedBin, 'counter.schema.json', '--out', 'counter.generated.js'], consumer, 'installed CLI through npm bin symlink');
  await writeFile(join(consumer, 'cli.mjs'), `import assert from 'node:assert/strict';\nimport { Counter } from './counter.generated.js';\nimport { ReactiveObject, ReactivePropertyValidationError } from ${JSON.stringify(importName)};\nconst model = new Counter();\nassert(model instanceof ReactiveObject);\nassert.equal(model.Count, 5);\nmodel.Count = 8;\nassert.equal(model.Count, 8);\nassert.throws(() => { model.Count = -1; }, ReactivePropertyValidationError);\nmodel.Dispose();\n`);
  run(process.execPath, ['cli.mjs'], consumer, 'installed CLI generated JavaScript consumer');
  console.log('Installed npm CLI, generated JavaScript, and validation: passed');
  console.log(`Verified actual ${packageJson.name}@${packageJson.version} tarball (${packedFiles.size} files).`);
} finally {
  await rm(temporary, { recursive: true, force: true });
}
