# ReactiveWeb

[![CI and distribution](https://github.com/wieslawsoltes/ReactiveWeb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/wieslawsoltes/ReactiveWeb/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/%40wieslawsoltes%2Freactiveweb)](https://www.npmjs.com/package/@wieslawsoltes/reactiveweb)
[![npm downloads](https://img.shields.io/npm/dm/%40wieslawsoltes%2Freactiveweb)](https://www.npmjs.com/package/@wieslawsoltes/reactiveweb)
[![Latest release](https://img.shields.io/github/v/release/wieslawsoltes/ReactiveWeb)](https://github.com/wieslawsoltes/ReactiveWeb/releases/latest)
[![License](https://img.shields.io/github/license/wieslawsoltes/ReactiveWeb)](LICENSE)
[![Live demo](https://img.shields.io/badge/demo-GitHub%20Pages-blue)](https://wieslawsoltes.github.io/ReactiveWeb/)

**Reactive MVVM for JavaScript, TypeScript, HTML and React, powered by RxJS.**

[Interactive showcase](https://wieslawsoltes.github.io/ReactiveWeb/) ·
[Releases and full source](https://github.com/wieslawsoltes/ReactiveWeb/releases) ·
[API compatibility](docs/compatibility.md) · [Generation guide](docs/generation.md) ·
[DynamicData integration](docs/dynamic-data.md)

ReactiveWeb brings familiar `ReactiveObject`, `WhenAnyValue`, `ReactiveCommand`,
`Interaction`, `WhenActivated`, and `RoutingState` patterns to the web. The reference
is **ReactiveUI.Reactive**, the System.Reactive-compatible distribution. The runtime
uses the actual **RxJS 7.8** observable, subject, subscription, scheduler and operator
implementations. It does not provide a separate home-grown observable engine.

The API keeps PascalCase names for .NET application ports, offers selected
JavaScript aliases, ships strong TypeScript declarations, and separates the core
from HTML and optional React adapters. Reactive properties can be explicit
accessors, standard TypeScript accessor decorators, schema-generated classes, or
readable source produced at build time.

This is an independent MIT-licensed implementation. **Version 0.2.0 implements the
documented browser MVVM surface; it does not claim exhaustive API or behavioral
equivalence to every ReactiveUI package and native platform.** See the pinned
[upstream audit](docs/upstream-audit.md) and detailed [mapping](docs/compatibility.md).

## Get the code and run the showcase

```sh
git clone https://github.com/wieslawsoltes/ReactiveWeb.git
cd ReactiveWeb
npm ci
npm run check
npm run dev
```

Open `http://localhost:4173`. The built showcase uses local relative assets and
works under the GitHub Pages `/ReactiveWeb/` path. It includes light/dark themes,
responsive layouts, editable view models, live state, event streams and source
examples for the main API families.

## Install the library

Node consumers and build tools require **Node 22 or newer**. Install the public npm
package alongside RxJS:

```sh
npm install @wieslawsoltes/reactiveweb rxjs
# Only when using the /react adapter:
npm install react react-dom
```

Every versioned GitHub release also contains an npm-compatible `.tgz`:

```sh
npm install ./wieslawsoltes-reactiveweb-0.2.0.tgz rxjs
```

The release pipeline publishes the same verified package to npmjs and GitHub
Packages. GitHub Packages consumers must configure
`@wieslawsoltes:registry=https://npm.pkg.github.com` and authenticate according to
GitHub's npm-registry requirements. Public npmjs installation requires no registry
configuration. See [publishing](docs/publishing.md) for release and credential setup.

| Entry point | Purpose |
| --- | --- |
| `@wieslawsoltes/reactiveweb` | Observable model, commands, lifetimes, routing, validation, collections, setup |
| `@wieslawsoltes/reactiveweb/html` | DOM bindings, reactive custom elements and view hosts |
| `@wieslawsoltes/reactiveweb/react` | React hooks, contexts and view hosts; React is optional |
| `@wieslawsoltes/reactiveweb/generation` | Decorators and typed runtime schema generation |
| `@wieslawsoltes/reactiveweb/dynamic-data` | Full `DynamicData` namespace and collection/change-set adapters |
| `@wieslawsoltes/reactiveweb/generator` | Node-only source-generator API |

The package contains ESM, CommonJS, declarations, source maps and original source.
Each module format preserves shared class identity between its entry points.
Keep a consumer on one module format; mixing ESM and CommonJS copies creates
separate module state, as with ordinary dual-format Node packages.

## A complete view model in ordinary JavaScript

```js
import {
  defineViewModel, reactiveProperty, WhenAnyValue,
  CompositeDisposable, DisposeWith
} from '@wieslawsoltes/reactiveweb';
import { Bind, OneWayBind, BindCommand } from '@wieslawsoltes/reactiveweb/html';

const ProfileViewModel = defineViewModel({
  properties: {
    FirstName: reactiveProperty('Alex'),
    LastName: reactiveProperty('Morgan')
  },
  computed: {
    FullName: {
      initialValue: '',
      source: vm => WhenAnyValue(vm, 'FirstName', 'LastName',
        (first, last) => `${first} ${last}`.trim())
    }
  },
  commands: {
    Save: {
      kind: 'task',
      canExecute: vm => WhenAnyValue(vm, 'FirstName', name => !!name.trim()),
      execute: async (vm, _input, signal) => {
        const response = await fetch('/api/profile', {
          method: 'POST', signal,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ first: vm.FirstName, last: vm.LastName })
        });
        if (!response.ok) throw new Error(`Save failed: ${response.status}`);
        return response.json();
      }
    }
  }
});

const vm = new ProfileViewModel();
const lifetime = new CompositeDisposable();
lifetime.Add(vm);
DisposeWith(Bind(vm, 'FirstName', document.querySelector('#first'), 'value'), lifetime);
DisposeWith(OneWayBind(vm, 'FullName', document.querySelector('#name'), 'textContent'), lifetime);
DisposeWith(BindCommand(vm.Save, document.querySelector('#save')), lifetime);
lifetime.Add(vm.Save.ThrownExceptions.subscribe(error => console.error(error)));

// Call this when the owning view is removed:
// lifetime.Dispose();
```

`/api/profile` is your application endpoint. The showcase uses a local asynchronous
operation so every example runs on static hosting without a server.

## Class-based .NET-style usage

```ts
import { ReactiveObject, Reactive, ReactiveCommand, WhenAnyValue, ToProperty }
  from '@wieslawsoltes/reactiveweb';

class PersonViewModel extends ReactiveObject {
  @Reactive accessor FirstName = 'Alex';
  @Reactive accessor LastName = 'Morgan';

  private readonly fullName = ToProperty(
    WhenAnyValue(this, 'FirstName', 'LastName', (a: string, b: string) => `${a} ${b}`),
    this, 'FullName', { initialValue: '' }
  );
  get FullName(): string { return this.fullName.Value; }

  readonly Reset = ReactiveCommand.Create(() => {
    const batch = this.DelayChangeNotifications();
    try { this.FirstName = 'Alex'; this.LastName = 'Morgan'; }
    finally { batch.Dispose(); }
  });

  override Dispose() {
    this.Reset.Dispose();
    this.fullName.Dispose();
    super.Dispose();
  }
}
```

Use standard TypeScript 5+ decorators with `accessor`; legacy
`experimentalDecorators` is a different decorator system. For ordinary JavaScript,
write `get Name() { return this.GetValue('Name', ''); }` and
`set Name(value) { this.RaiseAndSetIfChanged('Name', value); }`, or use the schema
factory above. Property reads/writes use normal accessors; no Proxy is required in
the generated property hot path.

## HTML custom elements

```ts
import { ReactiveElement } from '@wieslawsoltes/reactiveweb/html';

class PersonView extends ReactiveElement<PersonViewModel> {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' }).innerHTML = `
      <label>First name <input data-rx-bind="FirstName"></label>
      <strong data-rx-text="FullName"></strong>
      <button data-rx-command="Reset">Reset</button>
    `;
  }
}
customElements.define('person-view', PersonView);
const element = document.createElement('person-view') as PersonView;
element.ViewModel = new PersonViewModel();
document.body.append(element);
```

Bindings activate on connection, stop on disconnection, and rebind on view-model
replacement. A view does not automatically own the lifetime of its assigned model;
dispose models according to your application's ownership. Declarative bindings
resolve member paths, never evaluate arbitrary JavaScript strings. Text binding
uses `textContent`. See [HTML, React, validation and converter APIs](docs/integrations.md).

## React

```tsx
import { useReactiveObject, useReactiveCommand, useWhenActivated }
  from '@wieslawsoltes/reactiveweb/react';

function Counter({ viewModel }) {
  const vm = useReactiveObject(viewModel);
  const increment = useReactiveCommand(vm.Increment);
  useWhenActivated(vm); // vm provides Activator: ViewModelActivator
  return <button disabled={!increment.canExecute}
    onClick={() => increment.execute(undefined)}>
    Count: {vm.Count}
  </button>;
}
```

`useObservable` and `useReactiveObject` integrate with `useSyncExternalStore`.
Server rendering does not start subscriptions. Activation effects own disposable
leases and are tested under React StrictMode. Vue, Svelte, Angular, Lit and other
frameworks can consume the core RxJS streams or the custom elements; dedicated
framework-specific adapters for those frameworks are not included.

## Build-time property generation

```json
{
  "className": "CounterViewModel",
  "properties": {
    "Count": { "type": "number", "initial": 0, "validate": { "minimum": 0 } }
  }
}
```

```sh
npx reactiveweb-generate counter.schema.json --out CounterViewModel.ts
# Use .js output for an ESM JavaScript class.
```

Generated output is readable source with explicit accessors. Named imports supply
computed-property factories and command bodies. Invalid schemas and member
collisions fail early; overwriting existing output requires `--force`.
See [generation](docs/generation.md) and [compilable examples](examples/generation).

## Available feature families

| Area | APIs and behavior |
| --- | --- |
| Objects and observation | `Changing`, `Changed`, `RaiseAndSetIfChanged`, suppression, delayed batching, nested `WhenAnyValue`/`WhenAny`/`WhenAnyObservable`, property providers |
| Observable properties | `ToProperty`, deferred `ObservableAsPropertyHelper`, `ReactiveProperty`, scheduler selection, error and validation streams |
| Commands | Sync, Promise/AbortSignal, Observable and combined commands, cold execution, concurrency accounting, exception streams, gated `InvokeCommand` |
| Lifetimes | Disposable, composite, serial, single-assignment, reference counting, `ViewModelActivator`, `WhenActivated` |
| Interactions | Typed input/output, newest-first handlers, async/Observable handlers, cancellation and unhandled errors |
| Routing | Navigation stack, navigate/back/reset commands, view locator contracts, HTML and React hosts, enter/leave resource scopes |
| Validation | Property and stream validation, async obsolete-result cancellation, aggregate pending/errors, command gating |
| Collections | DynamicDataWeb list/cache pipelines, property refresh, incremental collection binding, filtering/sorting/paging, HTML/React views and per-object resource ownership |
| Services and messages | Factory/constant/lazy-singleton registrations, contracts, reversible registrations, live/latest typed message channels |
| Persistence | Suspension lifecycle, memory/localStorage drivers, JSON migration hooks, serial saves, AutoPersist/AutoPersistCollection |
| Setup and conversion | `RxAppBuilder`, modular facades, property-provider registration, affinity-based converters, binding hooks |
| Scheduler/operator helpers | `ScheduledSubject`, `WaitForDispatcherScheduler`, switching subscriptions, logged catch and observation operators |
| Generation | Standard accessor decorators, runtime schema classes, TypeScript/JavaScript CLI source generation |

Read [core API usage](docs/api.md), [application setup](docs/setup.md),
[observable helpers](docs/observables.md), and the [compatibility matrix](docs/compatibility.md)
for exact contracts and adaptations.

## Buildless browser distribution

Extract `reactiveweb-browser.tar.gz` from a release and serve the entire directory,
including `browser-chunks/` and `third-party/`:

```html
<script type="module">
  import { ReactiveObject, rxjs } from './reactiveweb.browser.js';
  import { Bind } from './reactiveweb-html.browser.js';
  const vm = new ReactiveObject({ Name: 'Alex' });
  Bind(vm, 'Name', document.querySelector('input'), 'value');
  vm.WhenAnyValue('Name').pipe(rxjs.map(name => name.toUpperCase()))
    .subscribe(console.log);
</script>
```

The standalone entries share generated chunks to preserve class identity. RxJS is
bundled into that distribution, and its namespace is exported as `rxjs`. The npm
ESM/CommonJS entries keep RxJS external for normal application bundlers.

## Verification and releases

```sh
npm run check            # clean build, strict example types, behavioral tests, demo
npm run test:package     # actual npm tarball in an isolated consumer
npx playwright install chromium
npm run test:browser     # complete showcase interactions, desktop/mobile/dark
npm run benchmark       # measured local observations, not cross-framework claims
npm pack                # publishable .tgz
```

CI validates Node 22 and 24, compiles and executes generated source, checks installed
ESM/CommonJS consumers, exercises Chromium, and retains visual artifacts. Successful
`main` builds deploy GitHub Pages and publish a new package version to GitHub
Packages and GitHub Releases, then call the npm publication workflow with provenance.
The workflow verifies registry integrity and tests the downloaded npm tarball.
Increment the version and update release notes for a new release; existing release
assets are left unchanged. Manual publication of an existing release remains available.

## Platform and compatibility boundaries

JavaScript cannot preserve CLR binary interfaces, C# overload resolution,
expression-tree semantics, Roslyn diagnostics, native control hierarchies, or
operating-system suspension guarantees. Explicit tokens replace erased runtime
generic types, member paths replace expression trees, and web lifecycle adapters
replace native UI integrations. DynamicData collection pipelines are provided by the published
`@wieslawsoltes/dynamicdataweb` dependency and integrated with ReactiveWeb models,
collections, persistence, HTML and React. Its own overload and platform adaptations
remain documented in the [DynamicData integration guide](docs/dynamic-data.md).

The suite verifies the cases named in its tests. It is not an exhaustive upstream
differential conformance suite, production-scale soak test, or all-browser
certification. The [compatibility matrix](docs/compatibility.md) records the remaining
method/overload and platform boundaries, including intentional behavioral choices.

## License

[MIT](LICENSE). Upstream reference and dependency attributions are in [NOTICE](NOTICE).
