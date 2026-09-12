# Reactive properties and build-time generation

ReactiveWeb offers three ways to remove repetitive property code: standard TypeScript auto-accessor decorators, a JavaScript runtime class factory, and a build-time JSON-to-TypeScript/JavaScript generator. All three produce ordinary getters/setters and use the same RxJS-backed `ReactiveObject` notifications. Property access does not require a `Proxy`, reflection metadata, or an expression interpreter.

This is a web adaptation of the ReactiveUI.SourceGenerators workflow. It does not run Roslyn generators or claim compatibility with every .NET attribute, diagnostic, accessibility modifier, or native UI target.

## Choose a workflow

| Workflow | Use when | Build requirement |
|---|---|---|
| `@Reactive accessor Name = ''` | You already write TypeScript classes | TypeScript 5+ standard decorator transformation |
| `defineViewModel({...})` | You want plain JavaScript or declarative view models | None beyond normal ESM/RxJS resolution |
| `reactiveweb-generate schema.json --out model.ts` | You want explicit reviewable source committed to the repository | Node 22+ at generation time |

All view models expose .NET-style `GetValue`, `RaiseAndSetIfChanged`, `WhenAnyValue`, `Changed`, `Changing`, and `Dispose`. Commands use `ReactiveCommand` and observable computed properties use `ToProperty`/`ObservableAsPropertyHelper`.

## Standard TypeScript decorators

```ts
import { ReactiveObject, Reactive } from '@wieslawsoltes/reactiveweb';

export class PersonViewModel extends ReactiveObject {
  @Reactive accessor FirstName = 'Ada';

  @Reactive<string>({
    validate: value => value.trim().length > 0 || 'A last name is required.',
    dependents: ['DisplayName'],
  }) accessor LastName = 'Lovelace';

  get DisplayName() { return `${this.FirstName} ${this.LastName}`; }
}

const person = new PersonViewModel();
person.Changed.subscribe(change => {
  console.log(change.PropertyName, change.OldValue, change.Value);
});
person.LastName = 'Byron';
```

Use `accessor`, rather than a plain field. The decorator requires a public, non-static, string-named accessor on a `ReactiveObject` subclass. It rejects native/private fields and member names reserved by the reactive base class. The default initializer is seeded without publishing a change event, so the first later event has the correct old value.

Do **not** enable `experimentalDecorators` or `emitDecoratorMetadata` for this workflow. TypeScript's standard decorator transform has different signatures from its legacy decorators. A minimal setup is:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "lib": ["ES2022", "DOM", "ESNext.Decorators"]
  }
}
```

The `ReactivePropertyOptions<T>` contract is:

| Option | Behavior |
|---|---|
| `validate(value)` | Return `true` to accept; `false` or a string to throw `ReactivePropertyValidationError` before mutation or notification |
| `equals(previous, next)` | Custom equality; defaults to `Object.is` |
| `dependents` | Synchronous computed getter names that should receive before/after notifications when this property changes |

Validation is synchronous assignment validation. For error collections, UI error messages, and an observable valid/invalid state, use ReactiveWeb's validation APIs.

A dependent getter's old value is captured before the primary write. Its `Changing` notification precedes the primary change; its `Changed` notification follows it. Set `dependents` on **every input property** affecting that getter. This mechanism does not discover dependencies automatically and should not be added for a computed `ToProperty` member that already publishes its own notifications.

## Plain JavaScript class factory

```js
import { defineViewModel, reactiveProperty } from '@wieslawsoltes/reactiveweb';
import { firstValueFrom, map } from 'rxjs';

const Counter = defineViewModel({
  name: 'Counter',
  properties: {
    Count: reactiveProperty(1, { validate: value => value >= 0 }),
    Tags: reactiveProperty([]),
  },
  computed: {
    Double: {
      source: vm => vm.WhenAnyValue('Count').pipe(map(count => count * 2)),
      initialValue: 0,
    },
  },
  commands: {
    Add: { execute: (vm, amount) => vm.Count += amount },
    Load: {
      kind: 'task',
      execute: async (vm, url, signal) => {
        const response = await fetch(url, { signal });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      },
    },
  },
});

const vm = new Counter({ Count: 3 });
await firstValueFrom(vm.Add.Execute(2));
console.log(vm.Count, vm.Double); // 5, 10
vm.Dispose();
```

`defineViewModel` creates one constructor with prototype accessors; `DefineViewModel` is its .NET-style alias. Constructors accept overrides for declared reactive properties and reject unknown property names. Each instance owns separate reactive state, computed subscriptions, and commands. Array/object defaults are cloned with `structuredClone`. Supply `factory` for custom class instances or values needing special initialization:

```ts
const Model = defineViewModel({
  properties: {
    CreatedAt: reactiveProperty(new Date(0), { factory: () => new Date() }),
  },
});
```

TypeScript infers property types and contextually types factory callbacks against `ReactiveObject` plus the declared mutable properties. The returned instance also contains inferred computed and command members. A computed member without `initialValue` can be `undefined` before its first emission. Factory callbacks initialize in declaration order: all reactive properties, then computed members, then commands. If one computed factory needs another computed member, supply an explicit application type or compose the underlying streams directly.

Computed definitions support `source`, `initialValue`, `scheduler`, `deferSubscription`, and `comparer`, mirroring `ToProperty` options. Their installed properties are read-only cached values. `deferSubscription: true` starts observation on the first property read.

Command definitions support `kind: 'sync' | 'task' | 'observable'`, `execute(viewModel, input, signal?)`, `canExecute(viewModel)`, and `outputScheduler`. The default is synchronous execution. Task and observable callbacks receive an `AbortSignal`; they must cooperate with cancellation for underlying work to stop. The commands themselves stop observing a cancelled execution. `Execute` is cold: subscribe or use `firstValueFrom` to execute it.

`Dispose()` releases generated resources in reverse construction order, cancels generated command executions, and completes the view model's notification streams. If construction fails, already-created resources are released. Disposal is idempotent. A DOM/framework owner is responsible for deciding when the view model's lifetime ends.

## Build-time CLI

Install the package together with RxJS, then generate a class:

```sh
npm install @wieslawsoltes/reactiveweb rxjs
npx reactiveweb-generate counter.schema.json --out counter.generated.ts
```

For plain ESM JavaScript:

```sh
npx reactiveweb-generate counter.schema.json --out counter.generated.js
```

The CLI infers JavaScript from `.js`/`.mjs`; otherwise it generates TypeScript. `--language ts|js` overrides this inference. `--module package-or-path` changes the generated ReactiveWeb import. Existing outputs are preserved unless `--force` is supplied. The input schema cannot also be the output file. Schema errors produce a nonzero exit status.

```json
{
  "className": "CounterViewModel",
  "imports": {
    "doubleCount": "./counter-logic.js",
    "increment": "./counter-logic.js",
    "canIncrement": "./counter-logic.js"
  },
  "properties": {
    "Count": {
      "type": "number",
      "initial": 0,
      "validate": { "minimum": 0, "maximum": 1000 }
    }
  },
  "computed": {
    "Double": { "source": "doubleCount", "type": "number", "initialValue": 0 }
  },
  "commands": {
    "Increment": {
      "execute": "increment",
      "canExecute": "canIncrement",
      "inputType": "void",
      "outputType": "number"
    }
  }
}
```

An imported computed factory receives `(viewModel)` and returns an RxJS observable. An imported command handler receives `(viewModel, input)` for synchronous commands or `(viewModel, input, signal)` for `task`/`observable`. An imported `canExecute` factory receives `(viewModel)` and returns `Observable<boolean>`.

```ts
import { map } from 'rxjs';
import type { CounterViewModel } from './counter.generated.js';

export const doubleCount = (vm: CounterViewModel) =>
  vm.WhenAnyValue<number>('Count').pipe(map(count => count * 2));
export const canIncrement = (vm: CounterViewModel) =>
  vm.WhenAnyValue<number>('Count').pipe(map(count => count < 1000));
export const increment = (vm: CounterViewModel, _input: void) => ++vm.Count;
```

The generator emits explicit typed `get`/`set` declarations, assignment validation, `ToProperty` setup, typed `ReactiveCommand` fields, and deterministic cleanup. It references named imports; JSON does not contain executable expressions and the generator does not evaluate imported application modules.

| Schema field | Supported options |
|---|---|
| Top level | `className`, `imports`, `properties`, `computed`, `commands` |
| Reactive property | `initial`, `type`, `nullable`, `validate`, `dependents` |
| Computed property | `source`, `type`, `nullable`, `initialValue`, `deferSubscription` |
| Command | `execute`, `kind`, `canExecute`, `inputType`, `outputType` |
| Validation | `required`, `minimum`, `maximum`, `minLength`, `maxLength`, `pattern` |

Supported explicit value types are `string`, `number`, `boolean`, `object`, `unknown`, `string[]`, `number[]`, and `boolean[]`. Commands also support `void`. `nullable` adds `null` to property types. Without an explicit type, scalar default values infer scalar types; other defaults use `unknown`. Reactive properties require `initial`. JSON defaults must match any declared type. Generated setters also check explicit property types at runtime, including finite scalar numbers. Runtime validation rejects assignments, including invalid constructor overrides.

Schemas reject unknown options, duplicate/reserved member names, malformed identifiers, missing imported factories, invalid regular expressions, and contradictory numeric/length bounds. A valid schema can still reference an application module or factory with the wrong actual export signature: strict TypeScript compilation verifies those application-level contracts.

Programmatic generation is available from the Node-only entry point:

```ts
import { GenerateViewModelSource, ValidateGenerationSchema } from '@wieslawsoltes/reactiveweb/generator';

ValidateGenerationSchema(schema);
const source = GenerateViewModelSource(schema, { language: 'ts' });
```

## Typed HTML views and other frameworks

Use `ReactiveElement<TViewModel>` from `@wieslawsoltes/reactiveweb/html` as the typed HTML `IViewFor` host. `ViewModel` and its `DataContext` alias drive bindings; attaching/detaching the element activates/deactivates them. A complete custom-element example is in [`examples/generation/counter-view.ts`](../examples/generation/counter-view.ts). It binds a generated view model in a shadow root and disposes its owned model when the view is explicitly disposed.

The HTML base does not automatically own a supplied view model. This allows several views to share one model or a view to detach and reconnect. Framework adapters, including React hooks, can use the same generated classes; generation has no dependency on a particular rendering system.

## Examples and verification

From a checkout after `npm run build`:

```sh
node dist/generator-cli.js examples/generation/counter.schema.json --out examples/generation/counter.generated.ts --force
npx tsc -p examples/generation/tsconfig.json --noEmit
node examples/generation/runtime.mjs
npm test
```

Generation tests execute standard decorators, check old/new notification order, custom equality and rejected writes, prove mutable defaults are isolated, verify computed cleanup and asynchronous command cancellation, and exercise construction failures. They generate TypeScript, compile it in strict mode, import the emitted JavaScript, and execute its properties, computed values, and command. A separate CLI check runs generated JavaScript and verifies overwrite protection.

The implementation does not promise measured performance equivalence to a .NET source generator. Its design removes property-access proxy interception and emits direct setters; application benchmarks are required for a particular workload.

## Upstream mapping and boundaries

| Upstream generator family | ReactiveWeb adaptation |
|---|---|
| Reactive properties / additional property notifications | `@Reactive`, runtime property definitions, or generated explicit accessors with `dependents` |
| Observable-as-property | Runtime `computed`, CLI `computed`, or direct `ToProperty` |
| Reactive command | Runtime/CLI command declarations or direct `ReactiveCommand` factories |
| Typed `IViewFor` implementation | `ReactiveElement<T>`/React adapter with a generated or hand-written view model |
| `IReactiveObject` implementation | Extend `ReactiveObject`; no arbitrary inheritance mixin generator |
| Reactive collection / derived list generator families | Use the collection APIs explicitly; no matching generator attributes |
| Native view/control host generation | Web view-host adapters; no WinForms/XAML code emission |
| Roslyn analyzers, C# accessibility/partial/required modifiers, source diagnostics | Not implemented as .NET tooling; TypeScript and schema validation provide web-specific checks |

The source of the upstream feature families is [ReactiveUI.SourceGenerators](https://github.com/reactiveui/ReactiveUI.SourceGenerators). Standard decorator behavior and the legacy-decorator distinction are documented in [TypeScript 5.0 release notes](https://www.typescriptlang.org/docs/handbook/release-notes/typescript-5-0.html).
