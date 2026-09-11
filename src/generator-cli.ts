#!/usr/bin/env node
/** Build-time JSON schema generation. This module is a Node-only entry point. */
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { realpathSync } from 'node:fs';
import { dirname, resolve, extname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { ReactiveObject as ReactiveObjectBase } from './reactive-object.js';

export type GeneratedValueType = 'string' | 'number' | 'boolean' | 'object' | 'unknown' | 'string[]' | 'number[]' | 'boolean[]';
export interface GeneratedValidation {
  required?: boolean;
  minimum?: number;
  maximum?: number;
  minLength?: number;
  maxLength?: number;
  pattern?: string;
}
export interface GeneratedPropertySchema {
  initial: unknown;
  type?: GeneratedValueType;
  nullable?: boolean;
  validate?: GeneratedValidation;
  dependents?: string[];
}
export interface GeneratedComputedSchema {
  source: string;
  type?: GeneratedValueType;
  nullable?: boolean;
  initialValue?: unknown;
  deferSubscription?: boolean;
}
export interface GeneratedCommandSchema {
  execute: string;
  kind?: 'sync' | 'task' | 'observable';
  canExecute?: string;
  inputType?: GeneratedValueType | 'void';
  outputType?: GeneratedValueType | 'void';
}
export interface GenerationSchema {
  className: string;
  /** Map of local factory/export names to their module specifiers. */
  imports?: Record<string, string>;
  properties?: Record<string, GeneratedPropertySchema>;
  computed?: Record<string, GeneratedComputedSchema>;
  commands?: Record<string, GeneratedCommandSchema>;
}
export interface GenerationOptions {
  language?: 'ts' | 'js';
  moduleName?: string;
}

const allowedTypes = new Set(['string', 'number', 'boolean', 'object', 'unknown', 'string[]', 'number[]', 'boolean[]']);
const reserved = new Set(['await', 'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger', 'default', 'delete', 'do', 'else', 'enum', 'export', 'extends', 'false', 'finally', 'for', 'function', 'if', 'implements', 'import', 'in', 'instanceof', 'interface', 'let', 'new', 'null', 'package', 'private', 'protected', 'public', 'return', 'static', 'super', 'switch', 'this', 'throw', 'true', 'try', 'typeof', 'var', 'void', 'while', 'with', 'yield']);
const reservedMembers = new Set(['constructor', '__proto__', 'prototype', 'Dispose', 'dispose', 'unsubscribe', 'Changed', 'Changing', 'PropertyChanged', 'PropertyChanging', 'ThrownExceptions', 'GetValue', 'RaiseAndSetIfChanged', 'RaisePropertyChanged', 'RaisePropertyChanging', 'SuppressChangeNotifications', 'DelayChangeNotifications', 'AreChangeNotificationsEnabled', '__generatedResources', '__generatedDisposed']);
const baseProbe = new ReactiveObjectBase();
for (const name of Object.getOwnPropertyNames(baseProbe)) reservedMembers.add(name);
baseProbe.Dispose();
for (const name of Object.getOwnPropertyNames(ReactiveObjectBase.prototype)) reservedMembers.add(name);
const reservedImports = new Set(['ReactiveObject', 'ReactiveCommand', 'ToProperty', 'ReactivePropertyValidationError']);
function record(value: unknown, label: string): asserts value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new TypeError(`${label} must be an object.`);
}
function identifier(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string' || !/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(value) || reserved.has(value)) {
    throw new TypeError(`${label} must be a non-reserved JavaScript identifier.`);
  }
}
function typeName(value: unknown, label: string, allowVoid = false): void {
  if (value !== undefined && (typeof value !== 'string' || (!allowedTypes.has(value) && !(allowVoid && value === 'void')))) {
    throw new TypeError(`${label} must be a supported scalar, object, or primitive-array type.`);
  }
}
function rejectUnknown(value: object, keys: string[], label: string): void {
  for (const key of Object.keys(value)) if (!keys.includes(key)) throw new TypeError(`Unknown ${label} option '${key}'.`);
}
function booleanOption(value: unknown, label: string): void {
  if (value !== undefined && typeof value !== 'boolean') throw new TypeError(`${label} must be boolean.`);
}
function validInitial(initial: unknown, type: string | undefined, nullable: boolean | undefined, label: string): void {
  if (initial === null && nullable) return;
  if (!type || type === 'unknown') return;
  const matches = type.endsWith('[]')
    ? Array.isArray(initial) && initial.every(value => typeof value === type.slice(0, -2))
    : type === 'object' ? initial !== null && typeof initial === 'object' && !Array.isArray(initial) : typeof initial === type;
  if (!matches || (typeof initial === 'number' && !Number.isFinite(initial))) throw new TypeError(`${label} does not match its declared type '${type}'.`);
}

/** Validates names, imported factories, types, validation rules, and collisions. */
export function ValidateGenerationSchema(value: unknown): asserts value is GenerationSchema {
  record(value, 'Schema');
  rejectUnknown(value, ['className', 'imports', 'properties', 'computed', 'commands'], 'schema');
  identifier(value.className, 'className');
  if (reservedImports.has(value.className)) throw new TypeError('className conflicts with a runtime import.');
  const imports = value.imports ?? {};
  record(imports, 'imports');
  for (const [name, moduleName] of Object.entries(imports)) {
    identifier(name, 'Imported name');
    if (reservedImports.has(name) || name === value.className) throw new TypeError(`Imported name '${name}' conflicts with a generated declaration.`);
    if (typeof moduleName !== 'string' || !moduleName.trim() || /[\r\n\0]/.test(moduleName)) throw new TypeError(`Invalid module for '${name}'.`);
  }
  const allNames = new Set<string>();
  const requireFactory = (factory: unknown, label: string) => {
    identifier(factory, label);
    if (!Object.hasOwn(imports, factory)) throw new TypeError(`${label} '${factory}' is missing from imports.`);
  };
  for (const section of ['properties', 'computed', 'commands'] as const) {
    const members = value[section] ?? {};
    record(members, section);
    for (const [name, raw] of Object.entries(members)) {
      identifier(name, `${section} member name`);
      if (allNames.has(name) || reservedMembers.has(name)) throw new TypeError(`Duplicate or reserved member '${name}'.`);
      allNames.add(name);
      record(raw, `${section}.${name}`);
      if (section === 'properties') {
        rejectUnknown(raw, ['initial', 'type', 'nullable', 'validate', 'dependents'], `property '${name}'`);
        if (!Object.hasOwn(raw, 'initial')) throw new TypeError(`Property '${name}' requires initial.`);
        typeName(raw.type, `${name}.type`);
        booleanOption(raw.nullable, `${name}.nullable`);
        validInitial(raw.initial, raw.type as string | undefined, raw.nullable as boolean | undefined, name);
        if (raw.dependents !== undefined) {
          if (!Array.isArray(raw.dependents)) throw new TypeError(`${name}.dependents must be an array.`);
          for (const dependent of raw.dependents) identifier(dependent, 'Dependent name');
        }
        if (raw.validate !== undefined) {
          record(raw.validate, `${name}.validate`);
          rejectUnknown(raw.validate, ['required', 'minimum', 'maximum', 'minLength', 'maxLength', 'pattern'], `validation for '${name}'`);
          booleanOption(raw.validate.required, `${name}.validate.required`);
          for (const bound of ['minimum', 'maximum', 'minLength', 'maxLength']) {
            const boundValue = raw.validate[bound];
            if (boundValue !== undefined && (typeof boundValue !== 'number' || !Number.isFinite(boundValue) || (bound.endsWith('Length') && (!Number.isInteger(boundValue) || boundValue < 0)))) {
              throw new TypeError(`${name}.validate.${bound} must be a finite ${bound.endsWith('Length') ? 'nonnegative integer' : 'number'}.`);
            }
          }
          if (raw.validate.minimum !== undefined && raw.validate.maximum !== undefined && Number(raw.validate.minimum) > Number(raw.validate.maximum)) throw new TypeError(`${name}: minimum exceeds maximum.`);
          if (raw.validate.minLength !== undefined && raw.validate.maxLength !== undefined && Number(raw.validate.minLength) > Number(raw.validate.maxLength)) throw new TypeError(`${name}: minLength exceeds maxLength.`);
          if (raw.validate.pattern !== undefined) {
            if (typeof raw.validate.pattern !== 'string') throw new TypeError(`${name}.validate.pattern must be a regular-expression string.`);
            new RegExp(raw.validate.pattern);
          }
        }
      } else if (section === 'computed') {
        rejectUnknown(raw, ['source', 'type', 'nullable', 'initialValue', 'deferSubscription'], `computed '${name}'`);
        requireFactory(raw.source, `${name}.source`);
        typeName(raw.type, `${name}.type`);
        booleanOption(raw.nullable, `${name}.nullable`);
        booleanOption(raw.deferSubscription, `${name}.deferSubscription`);
        if (Object.hasOwn(raw, 'initialValue')) validInitial(raw.initialValue, raw.type as string | undefined, raw.nullable as boolean | undefined, name);
      } else {
        rejectUnknown(raw, ['execute', 'kind', 'canExecute', 'inputType', 'outputType'], `command '${name}'`);
        requireFactory(raw.execute, `${name}.execute`);
        if (raw.canExecute !== undefined) requireFactory(raw.canExecute, `${name}.canExecute`);
        if (raw.kind !== undefined && !['sync', 'task', 'observable'].includes(String(raw.kind))) throw new TypeError(`${name}.kind must be sync, task, or observable.`);
        typeName(raw.inputType, `${name}.inputType`, true);
        typeName(raw.outputType, `${name}.outputType`, true);
      }
    }
  }
}

function literal(value: unknown): string {
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new TypeError('Initial values must be JSON serializable.');
  return serialized.replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
}
function inferredType(value: unknown): string {
  return typeof value === 'string' ? 'string' : typeof value === 'number' ? 'number' : typeof value === 'boolean' ? 'boolean' : 'unknown';
}
function validationCondition(_name: string, property: GeneratedPropertySchema, variable = 'value'): string[] {
  const rules: string[] = [];
  if (property.type && property.type !== 'unknown') {
    const type = property.type;
    const condition = type.endsWith('[]') ? `Array.isArray(${variable}) && ${variable}.every(item => typeof item === ${literal(type.slice(0, -2))})`
      : type === 'object' ? `${variable} !== null && typeof ${variable} === "object" && !Array.isArray(${variable})`
        : `typeof ${variable} === ${literal(type)}${type === 'number' ? ` && Number.isFinite(${variable})` : ''}`;
    rules.push(property.nullable ? `(${variable} === null || (${condition}))` : `(${condition})`);
  }
  const validation = property.validate;
  if (validation?.required) rules.push(`(${variable} !== null && ${variable} !== undefined && ${variable} !== "")`);
  if (validation?.minimum !== undefined) rules.push(`(typeof ${variable} === "number" && ${variable} >= ${validation.minimum})`);
  if (validation?.maximum !== undefined) rules.push(`(typeof ${variable} === "number" && ${variable} <= ${validation.maximum})`);
  if (validation?.minLength !== undefined) rules.push(`((typeof ${variable} === "string" || Array.isArray(${variable})) && ${variable}.length >= ${validation.minLength})`);
  if (validation?.maxLength !== undefined) rules.push(`((typeof ${variable} === "string" || Array.isArray(${variable})) && ${variable}.length <= ${validation.maxLength})`);
  if (validation?.pattern !== undefined) rules.push(`(typeof ${variable} === "string" && new RegExp(${literal(validation.pattern)}).test(${variable}))`);
  return rules;
}

/** Generates reviewable ESM source, without eval or embedding executable expressions from JSON. */
export function GenerateViewModelSource(schema: GenerationSchema, options: GenerationOptions = {}): string {
  ValidateGenerationSchema(schema);
  if (options.language !== undefined && options.language !== 'ts' && options.language !== 'js') throw new TypeError('language must be ts or js.');
  const ts = options.language !== 'js';
  const moduleName = options.moduleName ?? '@wieslawsoltes/reactiveweb';
  if (!moduleName || /[\r\n\0]/.test(moduleName)) throw new TypeError('moduleName must be a valid module specifier.');
  const properties = Object.entries(schema.properties ?? {});
  const computed = Object.entries(schema.computed ?? {});
  const commands = Object.entries(schema.commands ?? {});
  const coreImports = ['ReactiveObject', ...(computed.length ? ['ToProperty'] : []), ...(commands.length ? ['ReactiveCommand'] : []), ...(properties.some(([name, value]) => validationCondition(name, value).length) ? ['ReactivePropertyValidationError'] : [])];
  const lines = ['// Generated by reactiveweb-generate. Edit the schema and regenerate.', `import { ${coreImports.join(', ')} } from ${literal(moduleName)};`];
  const groupedImports = new Map<string, string[]>();
  for (const [name, source] of Object.entries(schema.imports ?? {}).sort(([a], [b]) => a.localeCompare(b))) {
    const group = groupedImports.get(source) ?? [];
    group.push(name);
    groupedImports.set(source, group);
  }
  for (const [source, names] of groupedImports) lines.push(`import { ${names.join(', ')} } from ${literal(source)};`);
  lines.push('', `export class ${schema.className} extends ReactiveObject {`, `  #generatedResources${ts ? ': { Dispose(): void }[]' : ''} = [];`, '  #generatedDisposed = false;');
  if (ts) {
    for (const [name, definition] of computed) lines.push(`  declare readonly ${name}: ${definition.type ?? inferredType(definition.initialValue)}${definition.nullable ? ' | null' : ''}${definition.initialValue === undefined ? ' | undefined' : ''};`);
    for (const [name, definition] of commands) lines.push(`  readonly ${name}: ReactiveCommand<${definition.inputType ?? 'unknown'}, ${definition.outputType ?? 'unknown'}>;`);
  }
  const initialType = ts ? `: Partial<Pick<${schema.className}, ${properties.map(([name]) => literal(name)).join(' | ') || 'never'}>>` : '';
  lines.push('', `  constructor(initial${initialType} = {}) {`, '    super();');
  lines.push(`    const allowedProperties = new Set(${literal(properties.map(([name]) => name))});`, '    for (const name of Object.keys(initial)) {', '      if (!allowedProperties.has(name)) throw new TypeError(`Unknown reactive property: ${name}`);', '    }', '    try {');
  for (const [name, definition] of properties) lines.push(`      this.${name} = Object.hasOwn(initial, ${literal(name)}) ? initial.${name}${ts ? '!' : ''} : ${literal(definition.initial)};`);
  for (const [name, definition] of computed) {
    const settings = `{ ${definition.initialValue !== undefined ? `initialValue: ${literal(definition.initialValue)}, ` : ''}deferSubscription: ${definition.deferSubscription ?? false} }`;
    lines.push(`      this.#generatedResources.push(ToProperty(${definition.source}(this), this, ${literal(name)}, ${settings}));`);
  }
  for (const [name, definition] of commands) {
    const method = definition.kind === 'task' ? 'CreateFromTask' : definition.kind === 'observable' ? 'CreateFromObservable' : 'Create';
    const asynchronous = method !== 'Create';
    const args = `input${ts ? `: ${definition.inputType ?? 'unknown'}` : ''}${asynchronous ? `, signal${ts ? ': AbortSignal' : ''}` : ''}`;
    const generic = ts ? `<${definition.inputType ?? 'unknown'}, ${definition.outputType ?? 'unknown'}>` : '';
    lines.push(`      this.${name} = ReactiveCommand.${method}${generic}((${args}) => ${definition.execute}(this, input${asynchronous ? ', signal' : ''})${definition.canExecute ? `, ${definition.canExecute}(this)` : ''});`, `      this.#generatedResources.push(this.${name});`);
  }
  lines.push('    } catch (error) {', '      this.Dispose();', '      throw error;', '    }', '  }');
  for (const [name, definition] of properties) {
    const type = `${definition.type ?? inferredType(definition.initial)}${definition.nullable ? ' | null' : ''}`;
    const rules = validationCondition(name, definition, 'candidate');
    lines.push('', `  get ${name}()${ts ? `: ${type}` : ''} { return this.GetValue${ts ? `<${type}>` : ''}(${literal(name)}); }`, `  set ${name}(value${ts ? `: ${type}` : ''}) {`);
    if (rules.length) {
      lines.push(`    const candidate${ts ? ': unknown' : ''} = value;`);
      rules.forEach((rule, index) => lines.push(`    const valid${index} = ${rule};`));
      lines.push(`    if (!(${rules.map((_, index) => `valid${index}`).join(' && ')})) throw new ReactivePropertyValidationError(${literal(name)}, value);`);
    }
    lines.push(`    if (Object.is(this.GetValue(${literal(name)}), value)) return;`);
    const dependents = [...new Set(definition.dependents ?? [])].filter(dependent => dependent !== name);
    dependents.forEach((dependent, index) => lines.push(`    const dependent${index} = Reflect.get(this, ${literal(dependent)});`));
    dependents.forEach((dependent, index) => lines.push(`    this.RaisePropertyChanging(${literal(dependent)}, dependent${index});`));
    lines.push(`    this.RaiseAndSetIfChanged(${literal(name)}, value);`);
    dependents.forEach((dependent, index) => lines.push(`    this.RaisePropertyChanged(${literal(dependent)}, dependent${index}, Reflect.get(this, ${literal(dependent)}));`));
    lines.push('  }');
  }
  lines.push('', `  ${ts ? 'override ' : ''}Dispose()${ts ? ': void' : ''} {`, '    if (this.#generatedDisposed) return;', '    this.#generatedDisposed = true;', `    const errors${ts ? ': unknown[]' : ''} = [];`, '    for (const resource of this.#generatedResources.splice(0).reverse()) {', '      try { resource.Dispose(); } catch (error) { errors.push(error); }', '    }', '    super.Dispose();', '    if (errors.length) throw new AggregateError(errors, "Generated view model disposal failed.");', '  }', '}', '');
  return lines.join('\n');
}

const usage = `Usage: reactiveweb-generate <schema.json> --out <view-model.ts|js> [--language ts|js] [--module package] [--force]\n\nGenerate a reactive class with properties, computed observables, and commands.\nExisting files are preserved unless --force is supplied.\n`;

/** CLI implementation exported separately so invocation/validation can be tested. */
export async function RunGenerator(args: readonly string[], io: { stdout: (text: string) => void; stderr: (text: string) => void } = { stdout: text => process.stdout.write(text), stderr: text => process.stderr.write(text) }): Promise<number> {
  try {
    if (args.includes('--help') || args.includes('-h')) { io.stdout(usage); return 0; }
    let input: string | undefined;
    let output: string | undefined;
    let language: 'ts' | 'js' | undefined;
    let moduleName: string | undefined;
    let force = false;
    const seen = new Set<string>();
    for (let index = 0; index < args.length; index++) {
      const arg = args[index]!;
      if (arg === '--force') { force = true; continue; }
      if (['--out', '--language', '--module'].includes(arg)) {
        if (seen.has(arg)) throw new TypeError(`Duplicate option ${arg}.`);
        seen.add(arg);
        const value = args[++index];
        if (!value || value.startsWith('--')) throw new TypeError(`Missing value for ${arg}.`);
        if (arg === '--out') output = value;
        if (arg === '--module') moduleName = value;
        if (arg === '--language') {
          if (value !== 'ts' && value !== 'js') throw new TypeError('--language must be ts or js.');
          language = value;
        }
      } else if (arg.startsWith('-')) throw new TypeError(`Unknown option ${arg}.`);
      else if (input) throw new TypeError('Only one schema input is accepted.');
      else input = arg;
    }
    if (!input || !output) throw new TypeError(usage.trim());
    if (resolve(input) === resolve(output)) throw new TypeError('Output must not overwrite the input schema.');
    if (!language) language = ['.js', '.mjs'].includes(extname(output)) ? 'js' : 'ts';
    const schema: unknown = JSON.parse(await readFile(input, 'utf8'));
    ValidateGenerationSchema(schema);
    const generated = GenerateViewModelSource(schema, { language, ...(moduleName ? { moduleName } : {}) });
    await mkdir(dirname(resolve(output)), { recursive: true });
    try { await writeFile(output, generated, { encoding: 'utf8', flag: force ? 'w' : 'wx' }); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'EEXIST') throw new Error(`Output already exists: ${output}. Use --force to replace it.`);
      throw error;
    }
    io.stdout(`Generated ${schema.className}: ${output}\n`);
    return 0;
  } catch (error) {
    io.stderr(`reactiveweb-generate: ${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }
}

function isMainModule(): boolean {
  try { return !!process.argv[1] && import.meta.url === pathToFileURL(realpathSync(resolve(process.argv[1]))).href; }
  catch { return false; }
}
if (isMainModule()) {
  void RunGenerator(process.argv.slice(2)).then(code => { process.exitCode = code; });
}
