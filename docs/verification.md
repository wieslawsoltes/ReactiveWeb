# Verification record

The initial 0.1.0 implementation was checked in a Linux container with Node
v24.19.0, TypeScript 5.9.3, RxJS 7.8.2, React 19.1.1 and Chromium through Playwright
1.55.0. This record describes local results; GitHub's workflow status is the
authoritative result for its separate Node 22/24 runners.

## Results

| Check | Result |
| --- | --- |
| Clean TypeScript library build and declarations | Passed |
| Strict demo and generation-example TypeScript | Passed |
| Node behavioral tests | 133 passed, zero failed/skipped |
| Actual installed npm tarball, ESM/CommonJS imports | Passed |
| Shared class identity between format-specific entry points | Passed |
| External peer RxJS identity | Passed |
| Strict ESM/CommonJS TypeScript consumers, negative type checks | Passed |
| Installed npm-bin CLI and generated JavaScript execution | Passed |
| Browser showcase interactions | 14 checks passed |
| Uncaught browser errors | Zero |
| Desktop, mobile and dark-theme layout | Rendered; no mobile horizontal overflow |

Behavioral cases include cold and overlapping commands, scheduler delivery,
cooperative task cancellation, source teardown, interaction handler precedence,
nested observation/replacement, suppression/coalescing, deferred properties,
stale validation results, activation failures and re-entry, singleton view reuse,
React StrictMode and SSR, same-turn invocation gating, collection rollback,
serialized persistence/invalidation, queued lease cleanup, provider affinity,
converter registration, generated-code compilation and invalid schemas.

Browser checks exercise every main showcase page: profile, nested properties,
commands, shadow-DOM components, activation, routing, interaction dialogs,
validation, collections, persistence across reload, message/service resolution,
generated invoice, and React. The final check captures desktop/dark/mobile images
in `test-results/`; CI retains these as verification artifacts.

## Example performance observation

One warmed-up run of `npm run benchmark` on the local Node v24.19.0 runtime recorded:

| Operation | Iterations | Elapsed | Approximate operations/second |
| --- | ---: | ---: | ---: |
| Reactive property write and subscribed observer | 100,000 | 66.33 ms | 1,507,666 |
| Schema-generated property write | 100,000 | 20.57 ms | 4,862,571 |
| Synchronous command execution | 20,000 | 166.33 ms | 120,246 |
| Batched collection add/remove | 10,000 | 6.22 ms | 1,607,190 |

These are local observations, not guarantees or comparisons to ReactiveUI/.NET,
other frameworks, browser rendering, or real application workloads. VM instance
construction, render cost, input distributions, garbage collection and hardware
can substantially change the results. The runnable script is included so a
consumer can measure its own environment.

## Limits of verification

Tests assert documented cases and adaptations. They are not a declaration-wide
cross-runtime differential comparison, complete ReactiveUI platform certification,
all-browser/device qualification, production-scale soak test or security audit.
Native .NET and Roslyn APIs are mapped or explicitly excluded in
[compatibility.md](compatibility.md), rather than counted as verified browser
features. Task/Promise cancellation requires cooperation and browser unload cannot
guarantee completion of asynchronous persistence.

## DynamicData integration (0.2.0)

The integration suite verifies legacy/native list changes, indexed duplicates,
cache keys and updates, property refresh, dynamic filtering/ordering, stable item
subscriptions, per-object lifetimes, persistence cancellation, atomic rollback,
empty sources, finite snapshots, reentrant subscription and terminal ordering.
HTML and React tests cover node retention, row cleanup, duplicate occurrences,
source replacement, StrictMode, SSR and synchronous completion. The browser
showcase check exercises a shared cache through both UI adapters.

Installed package consumers verify the `./dynamic-data` entry from ESM and
CommonJS, shared DynamicData class/RxJS identity, native ReactiveObject refresh and
strictly typed cache pipelines. Standalone assets retain the DynamicData license.
