# Upstream API and behavior audit

This document records the upstream contract used to design ReactiveWeb. It is a source-based inventory of feature families, not a declaration-by-declaration equivalence claim. The implementation status of those families is recorded separately in [compatibility.md](compatibility.md).

## Pinned sources

The audit inspected repository trees, public API snapshots, project files, and representative implementations on 2026-09-11. Links below pin the exact revisions so later upstream changes do not silently change the reference contract.

| Source | Inspected revision | Evidence |
| --- | --- | --- |
| ReactiveUI | `d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151` | [ReactiveUI.Reactive project](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Reactive/ReactiveUI.Reactive.csproj), [.Reactive .NET 8 public API](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Reactive/PublicAPI/net8.0/PublicAPI.txt), [neutral Core public API](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Core/PublicAPI/net8.0/PublicAPI.txt) |
| ReactiveUI.SourceGenerators | `6541806b9e58ccb4accf43637c0d397329968c9c` | [Attribute definitions](https://github.com/reactiveui/ReactiveUI.SourceGenerators/blob/6541806b9e58ccb4accf43637c0d397329968c9c/src/ReactiveUI.SourceGenerators.Roslyn/AttributeDefinitions.cs), [generator implementations](https://github.com/reactiveui/ReactiveUI.SourceGenerators/tree/6541806b9e58ccb4accf43637c0d397329968c9c/src/ReactiveUI.SourceGenerators.Roslyn) |
| ReactiveUI.Avalonia | `d207663b098cdbac9ee7bfd214f1221083766c3a` | [platform implementation](https://github.com/reactiveui/ReactiveUI.Avalonia/tree/d207663b098cdbac9ee7bfd214f1221083766c3a/src/ReactiveUI.Avalonia), [.Reactive project](https://github.com/reactiveui/ReactiveUI.Avalonia/blob/d207663b098cdbac9ee7bfd214f1221083766c3a/src/ReactiveUI.Avalonia.Reactive/ReactiveUI.Avalonia.Reactive.csproj) |

### Why the .Reactive variant matters

At this revision, `ReactiveUI.Reactive` compiles `src/ReactiveUI.Shared/**/*.cs` under `REACTIVE_SHIM`, moves the affected types into `ReactiveUI.Reactive`, references `ReactiveUI.Core`, and references `ReactiveUI.Primitives.Reactive`. Its public API exposes `System.Reactive.Unit`, `System.Reactive.Concurrency.IScheduler`, and `IObservable<T>`. Some underlying implementation classes use primitive broadcasters and sequencers, but the selected public seam is explicitly the System.Reactive-compatible one. Looking only in `src/ReactiveUI` would miss most of this implementation.

ReactiveWeb uses RxJS observables, subscriptions, subjects, operators, and schedulers at that seam. It does not reproduce the separate lightweight observable implementation or load a CLR. The appropriate comparison is observable behavior and MVVM API shape, with explicit JavaScript adaptations for language and platform facilities.

## Core feature families

| Upstream family | Public surface and behavior to preserve | Browser mapping |
| --- | --- | --- |
| Reactive objects | `ReactiveObject`, `IReactiveObject`, `IReactiveNotifyPropertyChanged`, `RaiseAndSetIfChanged`, property changing/changed streams, error stream, notification suppression and delay | JavaScript property descriptors, explicit setters, and RxJS event streams |
| Observed changes | `ObservedChange`, property-change event arguments, `ObservableForProperty`, `GetValue`, `Value` | Records with sender, path/property name, old and current value |
| Property observation | `WhenAnyValue`, `WhenAny`, dynamic variants, `WhenAnyObservable`; generated overloads through arity 12 | Property paths and selector functions; variadic/array input replaces generic overload families |
| Observable properties | `ObservableAsPropertyHelper`, `ToProperty`, deferred subscription, initial values, distinct values, changing/changed callbacks, errors, disposal | Cached getters driven by RxJS subscriptions |
| Reactive values | `ReactiveProperty<T>`, source-backed values, duplicate and initial-value options, validation, errors and refresh | Observable value holder with synchronous/asynchronous validation |
| Commands | `ReactiveCommand`, `ReactiveCommandBase`, `Create`, `CreateFromTask`, `CreateFromObservable`, `CreateRunInBackground`, `CreateCombined`, `InvokeCommand` | RxJS command with Promise/AbortSignal bridge, execution-state streams and command bindings |
| Interactions | `Interaction`, context input/output, synchronous/task/observable handler registration, handler removal, unhandled-interaction exception | Request/response observable with view-owned handler registration |
| View activation | `ViewModelActivator`, `WhenActivated`, `IActivatableViewModel`, `IActivatableView`, manual activation and provider affinity | DOM connection lifetime and framework mount/unmount lifetime |
| Property binding | `Bind`, `OneWayBind`, `BindTo`, `IReactiveBinding`, conversion delegates, trigger direction, binding hooks, converter registries | Disposable DOM/object bindings with explicit event and conversion configuration |
| Command/interaction binding | `BindCommand`, command parameters, explicit events, `BindInteraction`, rebinding when a property changes | DOM event binding and disposable interaction subscriptions |
| View resolution | `IViewFor`, `IViewLocator`, `DefaultViewLocator`, `ViewLocator`, contracts, single-instance/exclusion metadata, view mappings/modules | Constructor/symbol/string registrations and explicit view factories |
| Routing | `RoutingState`, `IScreen`, `IRoutableViewModel`, stack, navigation commands, `CurrentViewModel`, `NavigationChanges`, routing lifetime mixins | Observable VM stack with HTML host and optional URL/history bridge |
| Message bus | `MessageBus`, typed/contract channels, latest-value subscriptions, scheduler registration, message sources | Explicit JavaScript type/channel tokens and RxJS streams |
| Collections and comparison | `ReactiveChange`, `ReactiveChangeSet`, collection-change observation, count helpers, ordered/chained comparers | Observable collection records and JavaScript comparators, backed by the published DynamicDataWeb engine with cache/list bridges, UI adapters and lifecycle integration |
| Persistence and suspension | `AutoPersist`, metadata, manual save, collection persistence; `ISuspensionHost`, generic host, `ISuspensionDriver`, state creation/load/save/invalidation | JSON serialization and browser storage/lifecycle adapters |
| Scheduling and configuration | `RxSchedulers`, `RxState`, `RxSuspension`, `RxCacheSize`, `ScheduledSubject`, dispatcher scheduler, scoped builder | RxJS schedulers, injectable application configuration and scoped services |
| Dependency resolution | Resolver registrations, lazy constants, view modules, registration hooks, fluent `ReactiveUIBuilder`, instance isolation | Explicit dependency container, contracts and factories |
| Additional mixins | Null filtering, observable logging, observable/task functions, switching subscription helpers, observed-change helpers | RxJS operators and focused JavaScript helpers |

The .NET-neutral core contains extensive conversion APIs for nullable and non-nullable numeric types, GUIDs, URI, `DateOnly`, `TimeOnly`, `DateTime`, `DateTimeOffset`, and `TimeSpan`. These are a real part of the upstream public API. A small JavaScript converter registry must not be represented as an exact implementation of every conversion type, culture rule, numeric width, or nullability rule.

## Semantics requiring dedicated verification

### Property changes and nested observation

Equality must be checked before sending notifications. A changing notification occurs before the backing value changes and a changed notification afterward. Notification suppression and notification delay are distinct operations: suppression can discard notifications, while delayed notifications require explicit coalescing and delivery rules. Nested paths must unsubscribe from replaced objects and observe the new branch. A plain object without a notification mechanism cannot provide arbitrary future changes merely because a function reads its property. Relevant source: [reactive object extensions](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/ReactiveObject/IReactiveObjectExtensions.cs), [extension state](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/ReactiveObject/ExtensionState.cs).

`WhenAnyValue` emits the initial available value as well as subsequent changes. `WhenAny` projects observed-change records. `WhenAnyObservable` follows observable-valued properties and switches subscriptions when those properties are replaced. Multiple observable selectors without a result selector merge their values; the overload with a result selector combines the latest values. Null branches, property replacement, duplicate values, and disposal deserve explicit regression tests. Relevant source: [arity-one observation](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/WhenAny/WhenAnyMixins.Arity1.cs), [arity-two observable observation](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/WhenAny/WhenAnyObservableMixins.Arity2.cs).

An observable property helper owns its subscription, caches its value, can defer subscribing until value access, and exposes exceptions independently. Changing/changed callbacks must surround the update, not both run after it. Initial values, immediate synchronous emissions, equality behavior, scheduler selection, and disposing a deferred helper before access are separate cases. Source: [ObservableAsPropertyHelper](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/ObservableForProperty/ObservableAsPropertyHelper.cs).

### Command execution, errors, and cancellation

At the audited revision, `Execute(parameter)` is cold: each subscription starts an execution. Subscribing to the command itself observes aggregate results and does not start execution. Direct `Execute` is not guarded by `CanExecute`; command UI/invocation paths enforce gating. Concurrent direct executions keep `IsExecuting` true until the in-flight count reaches zero. Effective `CanExecute` is user permission combined with no executions in progress, and a supplied permission observable begins disabled until it emits. Source: [generic command implementation](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/ReactiveCommand/ReactiveCommand%7BTParam,TResult%7D.cs).

The output scheduler applies to aggregate command results, exceptions, and execution begin/end state. Results observed directly from `Execute` use the execution context. Exceptions must reach the execution observer and the command error stream; an unobserved command error uses the configured default handler. An error must not permanently terminate the command's aggregate result stream. `CreateCombined` has child-command eligibility and result-combination semantics in addition to forwarding errors. Sources: [command factories](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/ReactiveCommand/ReactiveCommand.cs), [combined command](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/ReactiveCommand/CombinedReactiveCommand.cs).

Unsubscribing from an observable execution tears down the observable. Cancelling a task requests cooperative cancellation; the audited upstream task execution remains in progress until the task actually settles. JavaScript `AbortSignal` has the same cooperative limitation: application work must observe it. Wrapping a Promise in an observable does not make the underlying work abortable. An RxJS asynchronous scheduler also does not move CPU work to another thread; actual parallel computation needs a Web Worker.

### Interactions and activation

Interaction handlers run in reverse registration order, permitting temporary view-local overrides. A handler may decline to produce output so an earlier handler can try. A completed chain with no output fails with `UnhandledInteractionException`. A context accepts one output. Sync, Promise/task, and observable completion are distinct from merely returning a value in JavaScript, so any convenience return-value behavior must be documented as an adaptation. Source: [Interaction](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/Interactions/Interaction.cs).

`ViewModelActivator` is reference-counted. Multiple activation handles share one active resource set. Disposing the final handle ends that activation; a later activation creates a new set. Forced deactivation must avoid stale handles disrupting a subsequent activation. Upstream activation corresponds to entering/leaving the visual tree, not simply a visibility flag. Source: [ViewModelActivator](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/Activation/ViewModelActivator.cs).

### Routing, bus, and persistence

The navigation stack's final element is the active VM. Navigate pushes, NavigateAndReset replaces the stack, and NavigateBack is eligible only with at least two entries. Hosts must establish the current VM when attaching and follow future changes. Replacing a router must disconnect the old one. The upstream router manages VMs rather than browser URL matching, so hash/history support is a platform adaptation. Source: [RoutingState](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/Routing/RoutingState.cs).

Message bus channels are keyed by .NET type and optional contract. JavaScript cannot infer erased generic types, so callers need an explicit stable token. `Listen` and `ListenIncludeLatest` have different replay behavior; scheduler registration and source disposal belong to the contract. Source: [MessageBus](https://github.com/reactiveui/ReactiveUI/blob/d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151/src/ReactiveUI.Shared/Routing/MessageBus.cs).

Persistence must distinguish observed property changes, filtering by persistence metadata, debouncing, manual saves, serialization, and driver errors. Browser page lifecycle events cannot guarantee completion of asynchronous work when a page/process exits. Restoring JSON does not automatically reconstruct arbitrary class instances, commands, subscriptions, cycles, or CLR serialization metadata.

## Source generator feature mapping

The upstream generator family reduces handwritten model/view boilerplate through Roslyn compilation. The following capabilities were found in the pinned [attribute definitions](https://github.com/reactiveui/ReactiveUI.SourceGenerators/blob/6541806b9e58ccb4accf43637c0d397329968c9c/src/ReactiveUI.SourceGenerators.Roslyn/AttributeDefinitions.cs).

| Generator | Upstream options/purpose | Appropriate JavaScript counterpart |
| --- | --- | --- |
| `Reactive` | Reactive fields/properties, dependent `AlsoNotify` names, setter/inheritance modifiers, required fields | Generated or runtime-installed descriptors with explicit dependencies and defaults |
| `ObservableAsProperty` | Observable field/property/method, generated property name, initial value, access/read-only/inheritance options | Observable-backed getter and helper ownership |
| `ReactiveCommand` | Method wrapping, `CanExecute`, output scheduler, background execution, access modifier | Command schema wrapping sync/Promise/observable functions |
| `IReactiveObject` | Add reactive-object implementation to a partial type | Class base, mixin, or explicit descriptor installer |
| `IViewFor` | Typed ViewModel/DataContext wiring and optional view/view-model registrations | HTMLElement/framework view adapter and explicit factory registration |
| `ReactiveCollection` | Reactive collection property generation | Observable collection defaults/factories |
| `BindableDerivedList` | Generated bindable derived-list accessor | Explicit derived collection factory/helper |
| `ViewModelControlHost`, `RoutedControlHost` | Generate WinForms host boilerplate | HTML view host and router host |

JavaScript descriptor/schema generation can avoid reflection and repeated proxy work on the property update path. Its existence alone does not establish performance superiority over hand-written accessors; reproducible benchmarks are needed. It also does not provide Roslyn analyzers, partial-class compilation, C# accessibility enforcement, arbitrary C# expression compilation, or the upstream diagnostic/suppression suite.

## Avalonia integration contract

The Avalonia integration is a useful reference for the HTML adapter, rather than a browser rendering engine to copy. Its [ReactiveUserControlBase](https://github.com/reactiveui/ReactiveUI.Avalonia/blob/d207663b098cdbac9ee7bfd214f1221083766c3a/src/ReactiveUI.Avalonia/ReactiveUserControlBase.cs) and corresponding window types bridge ViewModel/DataContext and activation. The property observation and command binding adapters connect Avalonia's property/event system to ReactiveUI.

[RoutedViewHost](https://github.com/reactiveui/ReactiveUI.Avalonia/blob/d207663b098cdbac9ee7bfd214f1221083766c3a/src/ReactiveUI.Avalonia/RoutedViewHost.cs) follows Router and ViewContract, resolves a view, assigns its ViewModel/DataContext, and uses DefaultContent for absent VMs or views. It owns subscriptions only while attached. [ViewModelViewHost](https://github.com/reactiveui/ReactiveUI.Avalonia/blob/d207663b098cdbac9ee7bfd214f1221083766c3a/src/ReactiveUI.Avalonia/ViewModelViewHost.cs) handles the direct VM-hosting case. Additional adapters include automatic data templates, suspension lifecycle, scheduler/error setup, and application/container builder extensions.

HTML analogues must handle connection/disconnection, changed view models, DOM event listeners, command disabled state, interaction handler cleanup, and view-factory contracts. React adapters additionally need stable external-store snapshots and subscription cleanup under repeated mount/unmount. Avalonia styled/direct properties, XAML compilation, native windows, routed-event semantics, and layout/rendering are not literally implementable by giving an HTMLElement the same class name.

## Scope of equivalence

ReactiveWeb can preserve API names, roles, and documented observable behaviors while using JavaScript type tokens, options objects, selectors, Promises, and standard web lifecycles. It cannot provide CLR binary compatibility, C# source compatibility, native controls, compiled expression trees, every .NET numeric/culture conversion, or System.Reactive scheduler/thread identity within a browser. Those are platform/language changes, not hidden completed features.

A complete equivalence claim would require a declaration-level inventory across target frameworks, an explicit mapping or exclusion for each declaration, upstream behavioral fixtures translated into differential tests, framework integration tests, and sustained lifecycle/performance verification. The present family audit is useful groundwork for that process and does not substitute for it.
