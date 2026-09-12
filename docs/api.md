# Core API guide

All examples use the real RxJS observable contract. Subscriptions expose
`unsubscribe()`; ReactiveWeb-owned resources also expose `.Dispose()`. Extension
methods generally become exported functions with the receiver as the first
argument, while common model APIs also have instance forms.

## Property notification and observation

```ts
const address = new ReactiveObject({ City: 'Warsaw' });
const customer = new ReactiveObject({ Address: address });
const subscription = WhenAnyValue(customer, 'Address.City').subscribe(console.log);

const replacement = new ReactiveObject({ City: 'Kraków' });
(customer as any).Address = replacement;
// Old address subscription is removed; replacement's initial City is emitted.

subscription.unsubscribe();
customer.Dispose();
address.Dispose();
replacement.Dispose();
```

For strongly typed application models, use explicit class accessors, standard
`@Reactive accessor`, or typed `defineViewModel` schemas. A dynamic
`new ReactiveObject(record)` creates runtime descriptors without inventing static
TypeScript members on the base class.

| API | Result |
| --- | --- |
| `GetValue(name, fallback?)` | Reads a backing value without invoking the public accessor |
| `RaiseAndSetIfChanged(name, value)` / `SetValue` | Compares with `Object.is`, emits Changing, updates, then emits Changed |
| `DefineReactiveProperty(vm, name, initial)` | Installs a regular reactive accessor; rejects member collisions |
| `Changing` / `Changed` | Events containing Sender, PropertyName, Value and OldValue |
| `ObservableForProperty(vm, path, options?)` | Observable property-event records |
| `WhenAnyValue(vm, ...paths, projector?)` | Initial/current values and subsequent distinct values |
| `WhenAny(vm, ...paths, projector)` | Event-record projection |
| `WhenAnyObservable(vm, ...paths, projector?)` | Switches nested observable properties; merges or combines selected streams |
| `SuppressChangeNotifications()` | Disposable nested notification-suppression scope |
| `DelayChangeNotifications()` | Disposable scope coalescing repeated property notifications |

Paths can be strings (`'Address.City'`), arrays, or member-selector functions.
Selectors represent member access, not arbitrary expression trees. Use string
paths plus a final projection or `.pipe(map(...))` when function arguments would
be ambiguous. A registered property provider can adapt another model system to
these streams; plain objects do not spontaneously notify on field assignment.

## Observable-backed properties

```ts
const lifetime = new CompositeDisposable();
const vm = new ReactiveObject({ Query: '' });
lifetime.Add(vm);
lifetime.Add(ToProperty(
  WhenAnyValue(vm, 'Query').pipe(map(query => String(query).length)),
  vm, 'Length', { initialValue: 0 }
));
```

`ToProperty` installs a read-only getter unless an existing read-only getter is
declared, in which case that getter should return the helper's `Value`.
`ObservableAsPropertyHelper` caches the latest value, applies equality checks and
the configured scheduler, and supplies `ThrownExceptions`. Setting
`deferSubscription: true` delays subscription until `Value` is first read.

When an object-valued initial value itself contains option-like keys, pass it
explicitly as `{ initialValue: object }` to avoid overload ambiguity.

`ReactiveProperty<T>` combines a mutable value and RxJS source/observer contracts:

```ts
const name = new ReactiveProperty('Alex');
name.AddValidationError(value => value.length >= 3 ? null : 'Too short');
name.ObserveErrorChanged.subscribe(errors => console.log(errors));
name.Subscribe(value => console.log(value));
name.Value = 'Al';
name.CheckValidation();
name.Dispose();
```

`AddValidator` returns a disposable registration. `AddValidationErrorObservable`
accepts a transformation of the persistent value stream, allowing debounce,
switching and history-sensitive validation. Promise validators suppress stale
results; observable validators are unsubscribed when replaced. `Refresh` emits
the current value explicitly, while `CheckValidation` revalidates without a
duplicate value event. Configure a scheduler explicitly when deferred delivery
is needed; the web default is synchronous.

## Commands

```ts
const canSave = new BehaviorSubject(true);
const save = ReactiveCommand.CreateFromTask(
  async (input: string, signal: AbortSignal) => {
    const response = await fetch(input, { signal });
    if (!response.ok) throw new Error(String(response.status));
    return response.text();
  }, canSave
);

save.CanExecute.subscribe(enabled => console.log({ enabled }));
save.IsExecuting.subscribe(busy => console.log({ busy }));
save.ThrownExceptions.subscribe(console.error);
const execution = save.Execute('/api/data').subscribe(console.log);
execution.unsubscribe(); // requests cooperative Promise cancellation
save.Dispose();
```

| Factory/API | Contract |
| --- | --- |
| `Create(fn, canExecute?, outputScheduler?)` | Synchronous function result |
| `CreateFromTask(fn, canExecute?, outputScheduler?)` | Promise-like result, receives AbortSignal |
| `CreateFromObservable(fn, canExecute?, outputScheduler?)` | ObservableInput factory with teardown/cancellation |
| `CreateCombined(commands, canExecute?, outputScheduler?)` | Requires children; combines latest child outputs |
| `Execute(input)` | Cold observable; each subscription executes independently |
| `Invoke(input)` | Gated invocation subscription |
| `InvokeCommand(command)` | Operator for event streams; ignores disallowed/busy events |
| `executeAsync(input)` | Promise for final execution result |

Explicit `Execute` does not consult `CanExecute` and can overlap. UI bindings and
invocation helpers enforce eligibility. A command's aggregate result stream,
execution state and exceptions use the output scheduler; the direct execution
observable forwards its source's execution stream.

A cancelled Promise must honor its AbortSignal to stop actual work. The command
tracks a non-cooperative Promise until it settles, suppressing cancelled outputs.
An Observable can provide teardown that stops its work immediately.

## Interactions and lifetime

```ts
const confirmation = new Interaction<string, boolean>();
const registration = confirmation.RegisterHandler(async context => {
  context.SetOutput(await presentConfirmation(context.Input));
});
const accepted = await firstValueFrom(confirmation.Handle('Apply changes?'));
registration.Dispose();
confirmation.Dispose();
```

Handlers run newest-first and may decline by returning without `SetOutput`.
Asynchronous handlers must complete before fallback continues. Output can be set
exactly once, including an explicit undefined value. No answering handler produces
`UnhandledInteractionError`. Each web Handle subscription receives a fresh context
and handler snapshot; this is an intentional difference from the audited upstream.

```ts
const vm = { Activator: new ViewModelActivator() };
WhenActivated(vm, scope => {
  scope.Add(interval(1000).subscribe(console.log));
});
const lease = vm.Activator.Activate();
lease.Dispose();
vm.Activator.Dispose();
```

Activation is reference-counted. The final lease releases resources. Re-entering
creates a new scope. `CompositeDisposable` disposes all members even if a member
throws; late additions are immediately disposed. Serial disposables replace and
dispose the previous value. `DisposeWith(resource, scope)` accepts RxJS teardown
and returns the original resource.

## Routing and views

```ts
const screen = { Router: new RoutingState() };
const details = { UrlPathSegment: 'details', HostScreen: screen };
screen.Router.Navigate.Execute(details).subscribe();
screen.Router.NavigateAndReset.Execute(details).subscribe();
screen.Router.NavigateBack.CanExecute.subscribe(console.log);
```

View factories are registered by view-model constructors and optional contracts.
`ViewLocator` can resolve a base-class registration for a derived instance.
Singleton view registrations own their cached views until unregistered; hosts
detach/reuse them. Ordinary view factories produce instances whose host owns their
view lifetime. The core router is a model stack; browser history synchronization
can be built separately from its observable state.

## Collections and persistence

```ts
const items = new ObservableCollection<TaskModel>();
const completed = new BindableDerivedList(items, {
  filter: item => item.Done,
  comparer: (left, right) => left.Title.localeCompare(right.Title)
});
items.Edit(list => { list.Add(firstTask); list.Add(secondTask); });
completed.ItemsChanged.subscribe(renderTasks);
```

Snapshots are immutable arrays; item objects remain application-owned.
`Edit` batches nested mutations and restores a failed edit. Derived lists now use
DynamicDataWeb refresh/filter/sort pipelines and accept ReactiveWeb collections,
DynamicData list/cache sources, and change-set observables. The
`ToDynamicDataChangeSet` and `BindChangeSet` adapters preserve the existing
ReactiveWeb change-record API while applying DynamicData batches incrementally.
See [DynamicData integration](dynamic-data.md) for sources, operators, UI bindings,
collection persistence and ownership contracts.

```ts
const driver = new LocalStorageSuspensionDriver('example.state');
const persistence = AutoPersist(vm,
  () => driver.SaveState({ Query: vm.Query }),
  { throttleMs: 300, onError: console.error });
await persistence.Flush();
persistence.Dispose();
```

AutoPersist serializes save work. `Flush` surfaces a save failure; later changes
can retry. Disposing cancels active observable saves and pending timers. JSON
state must be explicitly reconstructed into models/commands after loading.
`AutoPersistCollection` owns per-object persistence according to live object
references across ReactiveWeb collections and DynamicData list/cache/change-set inputs. `SuspensionHost` coordinates launch/resume/save/invalidate streams and
drivers; browser lifecycle events cannot guarantee asynchronous work at shutdown.

See [setup](setup.md), [integration](integrations.md), [observable helpers](observables.md)
and [generation](generation.md) for their complete implemented public surfaces.
