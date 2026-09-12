# Changelog

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
