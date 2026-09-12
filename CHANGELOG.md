# Changelog

## 0.2.0

Node-based consumers and build tools now require Node 22 or newer, matching the DynamicDataWeb dependency.

- Integrate the published `@wieslawsoltes/dynamicdataweb` collection engine with
  a shared RxJS peer and a dedicated `./dynamic-data` package entry.
- Add native list/cache change-set bridges and transactional delta binding while
  preserving the existing ObservableCollection change-record API.
- Use DynamicData refresh/filter/sort pipelines for BindableDerivedList with
  stable item subscriptions and observable filter/comparer criteria.
- Support DynamicData sources in per-object resource ownership and collection
  persistence, preserving duplicate-reference and cancellation semantics.
- Add HTML BindCollection with retained row identity and React
  useReactiveCollection/useCollection with stable snapshots and lifecycle cleanup.
- Add a live cache workspace sharing filters, sort, paging, aggregates and item
  edits between HTML and React views.
- Serialize reentrant collection notifications and preserve initial/terminal
  delivery ordering; extend package, lifecycle, UI and browser regression checks.
- Verify both explicit npm distribution tags and install-index availability,
  including a fresh anonymous package-name installation after publication.

## 0.1.1

* Upgrade development dependencies, including TypeScript 7, React 19.2, Playwright
  1.63 and esbuild 0.28, and update the GitHub Actions used for verification,
  package distribution and Pages deployment.
* Explicitly include Node declarations and isolate generated-code/package
  consumer compiler options for TypeScript 7 compatibility.
* Automatically publish each new verified release to npmjs with provenance,
  using the release tarball and validating its checksums.
* Verify public registry metadata, integrity and distribution tags, then download
  the published tarball and exercise ESM, CommonJS, TypeScript and generator CLI
  consumers. Repeated publication verifies existing bytes instead of overwriting.
* Retain the generated Pages hidden files in the deployed artifact.

The public library API and RxJS peer dependency are unchanged.

## 0.1.0

Initial public RxJS-based MVVM implementation with .NET-style APIs, reactive
objects and properties, nested observation, commands, interactions, activation,
routing, collections, validation, services, messaging, suspension and persistence.

Includes reusable HTML bindings and custom elements, React hooks and hosts,
standard accessor decorators, schema models and a build-time property generator.
Distributions include typed ESM, CommonJS, standalone browser modules, a feature
workbench, behavioral tests, package verification and release automation.

The upstream mapping and intentional adaptations are documented in
[compatibility.md](docs/compatibility.md). This version does not claim exhaustive
declaration or behavioral equivalence to every upstream package and platform.
