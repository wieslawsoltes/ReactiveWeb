import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { BehaviorSubject, firstValueFrom, map, Observable, of } from 'rxjs';
import { ReactiveObject } from '../dist/reactive-object.js';
import { Reactive, reactiveProperty, defineViewModel, ReactivePropertyValidationError } from '../dist/generation.js';
import { GenerateViewModelSource, RunGenerator, ValidateGenerationSchema } from '../dist/generator-cli.js';

class DecoratedPerson extends ReactiveObject {
  @Reactive accessor FirstName = 'Ada';
  @Reactive<string>({ dependents: ['FullName'], validate: value => value.trim().length > 0 || 'Name is required.' }) accessor LastName = 'Lovelace';
  @Reactive<{ id: number }>({ equals: (left, right) => left.id === right.id }) accessor Identity = { id: 1 };
  get FullName() { return `${this.FirstName} ${this.LastName}`; }
}

test('standard auto-accessors notify with correct initial old value, deduplicate and validate', () => {
  const person = new DecoratedPerson();
  const events: unknown[] = [];
  person.Changing.subscribe(event => events.push(['before', event.PropertyName, event.OldValue, person.LastName]));
  person.Changed.subscribe(event => events.push(['after', event.PropertyName, event.OldValue, event.Value]));
  person.LastName = 'Byron';
  assert.deepEqual(events, [
    ['before', 'FullName', 'Ada Lovelace', 'Lovelace'],
    ['before', 'LastName', 'Lovelace', 'Lovelace'],
    ['after', 'LastName', 'Lovelace', 'Byron'],
    ['after', 'FullName', 'Ada Lovelace', 'Ada Byron'],
  ]);
  person.LastName = 'Byron';
  person.Identity = { id: 1 };
  assert.equal(events.length, 4);
  assert.throws(() => { person.LastName = ''; }, ReactivePropertyValidationError);
  assert.equal(person.LastName, 'Byron');
  assert.equal(events.length, 4);
  person.Dispose();
});

test('schema class uses real accessors, isolates defaults, caches observables, and executes commands', async () => {
  const Counter = defineViewModel({
    name: 'Counter',
    properties: { Count: reactiveProperty(2, { validate: count => count >= 0 }), Items: reactiveProperty<string[]>([]) },
    computed: { Double: { source: vm => vm.WhenAnyValue<number>('Count').pipe(map(value => value * 2)), initialValue: 0 } },
    commands: { Add: { execute: (vm, amount: number) => vm.Count += amount } },
  });
  const one = new Counter();
  const two = new Counter({ Count: 10 });
  assert.equal(Counter.name, 'Counter');
  assert.equal(typeof Object.getOwnPropertyDescriptor(Counter.prototype, 'Count')?.get, 'function');
  assert.equal(one.Double, 4);
  assert.equal(await firstValueFrom(one.Add.Execute(3)), 5);
  assert.equal(one.Count, 5);
  assert.equal(one.Double, 10);
  one.Items.push('owned');
  assert.deepEqual(two.Items, []);
  assert.equal(two.Count, 10);
  assert.throws(() => { one.Count = -1; }, ReactivePropertyValidationError);
  assert.throws(() => new Counter({ Missing: true } as any), /Unknown reactive property/);
  one.Dispose();
  two.Dispose();
});

test('schema owns computed subscriptions and cancels task commands on disposal', async () => {
  const input = new BehaviorSubject(7);
  let aborted = false;
  const Model = defineViewModel({
    properties: {},
    computed: { Current: { source: () => input, initialValue: 0 } },
    commands: {
      Wait: { kind: 'task', execute: (_vm, _input, signal) => new Promise<void>(() => signal.addEventListener('abort', () => { aborted = true; })) },
      Query: { kind: 'observable', execute: () => of(9) },
    },
  });
  const instance = new Model();
  assert.equal(input.observers.length, 1);
  assert.equal(await firstValueFrom(instance.Query.Execute()), 9);
  const pending = instance.Wait.Execute().subscribe();
  instance.Dispose();
  assert.equal(input.observers.length, 0);
  assert.equal(aborted, true);
  assert.equal(pending.closed, true);
  instance.Dispose();
});

test('failed schema construction releases already-created computed subscriptions', () => {
  const input = new BehaviorSubject(1);
  const Model = defineViewModel({
    properties: {},
    computed: { First: { source: () => input }, Second: { source: () => { throw new Error('Factory failed'); } } },
  });
  assert.throws(() => new Model(), /Factory failed/);
  assert.equal(input.observers.length, 0);
});

test('schema generation rejects collisions, invalid names, missing factories, and invalid validation options', () => {
  for (const name of ['Changed', 'Changing', 'values', 'RaiseAndSetIfChanged', '__proto__', 'Dispose']) {
    assert.throws(() => defineViewModel({ properties: { [name]: reactiveProperty(0) } }), /Reserved|reserved/);
    assert.throws(() => ValidateGenerationSchema({ className: 'Example', properties: { [name]: { initial: 0 } } }), /reserved/);
  }
  assert.throws(() => ValidateGenerationSchema({ className: 'class' }), /identifier/);
  assert.throws(() => ValidateGenerationSchema({ className: 'Example', commands: { Save: { execute: 'missing' } } }), /missing from imports/);
  assert.throws(() => ValidateGenerationSchema({ className: 'Example', properties: { Age: { initial: -1, type: 'string' } } }), /declared type/);
  assert.throws(() => ValidateGenerationSchema({ className: 'Example', properties: { Age: { initial: 1, validate: { minimum: 5, maximum: 2 } } } }), /minimum exceeds/);
  assert.throws(() => ValidateGenerationSchema({ className: 'Example', typo: true }), /Unknown/);
});

test('generated TypeScript compiles strictly and executes properties, computed values, and task commands', async () => {
  const directory = await mkdtemp(resolve('.generation-compile-'));
  try {
    const source = GenerateViewModelSource({
      className: 'GeneratedCounter',
      imports: { doubled: './factories.js', add: './factories.js', canAdd: './factories.js' },
      properties: { Count: { initial: 2, type: 'number', validate: { minimum: 0, required: true } }, Tags: { initial: [], type: 'string[]' } },
      computed: { Double: { source: 'doubled', type: 'number', initialValue: 0 } },
      commands: { Add: { kind: 'task', execute: 'add', inputType: 'number', outputType: 'number', canExecute: 'canAdd' } },
    }, { moduleName: '../dist/index.js' });
    await writeFile(join(directory, 'generated.ts'), source);
    await writeFile(join(directory, 'factories.ts'), `import { map, of } from 'rxjs';\nimport type { GeneratedCounter } from './generated.js';\nexport const doubled = (vm: GeneratedCounter) => vm.WhenAnyValue<number>('Count').pipe(map(value => value * 2));\nexport const canAdd = (_vm: GeneratedCounter) => of(true);\nexport const add = async (vm: GeneratedCounter, input: number, signal: AbortSignal): Promise<number> => { if (signal.aborted) throw new Error('cancelled'); return vm.Count += input; };\n`);
    const compile = spawnSync(process.execPath, [resolve('node_modules/typescript/bin/tsc'), '--strict', '--target', 'ES2022', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--skipLibCheck', join(directory, 'generated.ts'), join(directory, 'factories.ts')], { encoding: 'utf8' });
    assert.equal(compile.status, 0, compile.stdout + compile.stderr);
    const { GeneratedCounter } = await import(pathToFileURL(join(directory, 'generated.js')).href);
    const counter = new GeneratedCounter();
    assert.equal(counter.Double, 4);
    assert.equal(await firstValueFrom(counter.Add.Execute(3)), 5);
    assert.equal(counter.Double, 10);
    assert.throws(() => { counter.Count = -1; }, ReactivePropertyValidationError);
    assert.throws(() => { counter.Count = 'bad'; }, ReactivePropertyValidationError);
    assert.throws(() => new GeneratedCounter({ Nope: 1 }), /Unknown reactive property/);
    counter.Dispose();
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('CLI writes runnable JavaScript and preserves existing files until --force', async () => {
  const directory = await mkdtemp(resolve('.generation-cli-'));
  try {
    const schema = join(directory, 'counter.json');
    const output = join(directory, 'counter.js');
    await writeFile(schema, JSON.stringify({ className: 'Counter', properties: { Count: { initial: 4, type: 'number' }, Token: { initial: 'value', type: 'string', validate: { pattern: '^value$' } } } }));
    const messages: string[] = [];
    const io = { stdout: (text: string) => messages.push(text), stderr: (text: string) => messages.push(text) };
    assert.equal(await RunGenerator([schema, '--out', output, '--module', '../dist/index.js'], io), 0);
    const generated = await readFile(output, 'utf8');
    assert.match(generated, /get Count\(\)/);
    assert.doesNotMatch(generated, /get Count\(\):/);
    const { Counter } = await import(pathToFileURL(output).href);
    const counter = new Counter();
    assert.equal(counter.Count, 4);
    assert.equal(counter.Token, 'value');
    assert.throws(() => { counter.Token = 'candidate'; }, ReactivePropertyValidationError);
    counter.Count = 8;
    assert.equal(counter.Count, 8);
    counter.Dispose();
    assert.equal(await RunGenerator([schema, '--out', output], io), 1);
    assert.match(messages.at(-1)!, /already exists/);
    assert.equal(await readFile(output, 'utf8'), generated);
    assert.equal(await RunGenerator([schema, '--out', output, '--force'], io), 0);
    assert.equal(await RunGenerator([schema, '--out', schema, '--force'], io), 1);
    assert.equal(await RunGenerator(['--help'], io), 0);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
