# DynamicDataWeb integration

ReactiveWeb uses `@wieslawsoltes/dynamicdataweb` as its collection engine. The npm
package is a production dependency, while RxJS remains the shared peer dependency.
The standalone browser distribution bundles both libraries and includes their
licenses. This integration is available from ReactiveWeb 0.2.0. Node-based consumers and
build tools require Node 22 or newer, matching DynamicDataWeb’s engine requirement.

## A reactive cache bound to a collection

```ts
import { ReactiveObject, Reactive } from '@wieslawsoltes/reactiveweb';
import { DynamicData, BindChangeSet } from '@wieslawsoltes/reactiveweb/dynamic-data';

class Task extends ReactiveObject {
  constructor(readonly Id: number, title: string) {
    super();
    this.Title = title;
  }
  @Reactive accessor Title = '';
  @Reactive accessor Done = false;
}

const tasks = new DynamicData.SourceCache<Task, number>(task => task.Id);
const binding = BindChangeSet(tasks.Connect().pipe(
  DynamicData.AutoRefresh<Task, number>(),
  DynamicData.Filter(task => !task.Done),
  DynamicData.Sort((left, right) => left.Title.localeCompare(right.Title)),
));
const subscription = binding.Collection.ItemsChanged.subscribe(console.log);
const task = new Task(1, 'Publish the library');
tasks.AddOrUpdate(task);
task.Done = true; // The row leaves the filtered result immediately.

subscription.unsubscribe();
binding.Dispose();
tasks.Dispose();
task.Dispose();
```

`DynamicData` exposes the complete public DynamicDataWeb namespace, including
cache/list sources, filtering, transforms, grouping, joins, sorting, paging,
virtualisation, aggregation and lifecycle operators. Its names retain their
DynamicDataWeb meanings. Keeping them in a namespace avoids collisions with
ReactiveWeb's existing `Bind`, `ChangeSet` and property-observation APIs.
The subpath also reexports DynamicDataWeb symbols. Its `ToObservableChangeSet`
name is the ReactiveWeb collection bridge; use `DynamicData.ToObservableChangeSet`
for DynamicDataWeb’s original stream conversion. The same package can also be
imported directly from `@wieslawsoltes/dynamicdataweb`.

## Bridging existing collections

`ToDynamicDataChangeSet(collection)` (also `ToObservableChangeSet`) converts a
ReactiveWeb collection to DynamicData list changes. It preserves indexed
occurrences, ranges, moves, replacement and refresh; a reset becomes a clear and
add-range batch. Existing `ObservableCollection.Connect()` callers continue to
receive the original ReactiveWeb records.

```ts
const source = new ObservableCollection<Task>();
const binding = BindChangeSet(ToDynamicDataChangeSet(source).pipe(
  DynamicData.Filter(task => !task.Done),
));
source.Edit(list => list.AddRange([first, second]));
```

`BindChangeSet(source, target?)` and `ToReactiveCollection(source, target?)`
return a disposable binding with `Collection`, `Errors` and `IsDisposed`.
Inputs may be ReactiveWeb collections, DynamicData list/cache sources, or
DynamicData change-set observables. Incoming batches are applied to a target
collection in one transaction. Cache key identity and duplicate list occurrences
are distinct from object identity. Dispose the binding when its owning view
leaves; caller-owned sources and item models retain their own lifetimes.

The binding reconciles a supplied target with its initial source contents.
Supplied targets remain alive when the binding is disposed or the source completes
or fails. A target created by the binding is disposed at those points, and the
binding's `IsDisposed` becomes true. Its final `Items` snapshot remains readable;
late `ItemsChanged` subscribers receive that final snapshot before completion.
The binding never disposes source collections or item models.

`ObservableCollection.ApplyChanges` accepts DynamicData changes, allowing the
DynamicDataWeb `Bind` operator to use the collection directly.

## Derived views and item changes

`BindableDerivedList` retains its existing public API and uses DynamicDataWeb
refresh/filter/sort operators. It accepts the same source families as
`BindChangeSet`. `SetFilter`, `SetComparer` and `Refresh` update the existing
pipeline. `filterObservable` and `comparerObservable` support observable criteria.
An optional `observeItem` selector supplies a custom item notification stream.

DynamicDataWeb recognizes ReactiveWeb's `Changed` and `PropertyChanged` event
shapes directly. Property-specific and unspecified-property notifications,
nested object replacement and branch unsubscription are supported. There is no
need to wrap a ReactiveObject in a second proxy or patch RxJS prototypes.

Items and snapshots remain separate: collection snapshots are immutable arrays;
mutable item models still expose their own property streams. A refresh may update
row content even when the collection's membership and ordering remain equal.

## HTML and React

`BindCollection` from `@wieslawsoltes/reactiveweb/html` renders collection sources
into a DOM element and retains unaffected row nodes during changes. The renderer
receives `(item, index, disposables)` and can register per-row bindings in that
scope. Options support a key selector and row updates; the returned binding has a
replaceable `Source` and a `Dispose` method.

`useReactiveCollection` (alias `useCollection`) from the React adapter supplies
stable immutable snapshots with subscriptions owned by the component lifecycle.
It supports source replacement and React StrictMode cleanup. An explicit server
snapshot can be supplied for rendering on the server.

See the DynamicData workspace in the showcase for a live cache, property-driven
filtering and ordering, pagination, aggregates, HTML rows and React consumers.

## Persistence and activation

`ActOnEveryObject` and `AutoPersistCollection` accept DynamicData sources and
change-set streams as well as existing ReactiveWeb collections. They own one
resource or persistence handle per distinct live object, even when that object
occurs more than once. Removing the final occurrence releases its handle.
`AutoPersistCollection.Flush` retains its asynchronous save/error contract.

Bindings and DynamicData subscriptions can be added to a `CompositeDisposable`
or activation scope. DynamicDataWeb also recognizes `.Dispose()`-only resources
returned from its subscription factories. Disposing a view's subscription does
not dispose a source that the application owns.

## Compatibility and performance

This closes ReactiveWeb's former separation from the DynamicData collection
engine. It does not establish exact equivalence to every DynamicData C# overload,
expression tree or native collection-binding context. DynamicDataWeb documents
those independent adaptations in its
[compatibility report](https://github.com/wieslawsoltes/DynamicDataWeb/blob/main/docs/COMPATIBILITY.md).

The engine retains state and emits change sets, while individual operator costs
vary. Sorting can shift arrays, and grouping or some list operations can scan or
rebuild retained state. DOM rendering and immutable snapshots also have a cost.
No constant-time claim applies to every pipeline or update.
