ReactiveWeb 0.2.0 integrates the published DynamicDataWeb engine into its RxJS MVVM APIs.

Node-based consumers and build tools require Node 22 or newer, matching DynamicDataWeb.

- Native SourceList/SourceCache and change-set pipelines connect to existing
  ReactiveWeb observable collections through transactional delta adapters.
- BindableDerivedList uses DynamicData property refresh, filtering and ordering
  while retaining unaffected item subscriptions and existing public methods.
- Collection persistence and resource ownership support DynamicData sources,
  including duplicate object references and asynchronous save cancellation.
- HTML BindCollection preserves row nodes and owns per-row resources; React
  useReactiveCollection/useCollection provides stable immutable snapshots.
- The new DynamicData showcase shares property edits, search, sort, paging and
  aggregates between HTML and React collection views.
- Reentrant initial, update and terminal notifications have regression coverage.
- Package tests verify shared DynamicData/RxJS identity across ESM and CommonJS,
  typed pipelines, native ReactiveObject property refresh and generator consumers.

Install:

```sh
npm install @wieslawsoltes/reactiveweb@0.2.0 rxjs
```

DynamicDataWeb 0.1.1 is installed as a production dependency. Its independent
DynamicData overload and platform adaptations remain documented; this release
closes ReactiveWeb’s collection integration boundary without claiming exhaustive
cross-runtime equivalence to every upstream overload.

Showcase: https://wieslawsoltes.github.io/ReactiveWeb/

Assets include the npm package, standalone browser modules, static showcase and
SHA-256 checksums, with full source archives supplied by GitHub.
