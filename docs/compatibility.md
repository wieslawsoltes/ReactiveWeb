# ReactiveUI-to-ReactiveWeb compatibility

ReactiveWeb provides a reusable JavaScript/TypeScript MVVM implementation built on RxJS, with .NET-style names and web-specific adapters. The reference is the **ReactiveUI.Reactive** public API, including its System.Reactive-compatible semantics. The pinned sources and research method are in [upstream-audit.md](upstream-audit.md).

This is a feature-family mapping for the delivered source. It is not a claim of complete declaration, behavior, source, or binary compatibility with every ReactiveUI target framework. Familiar names do not imply that C# overloads or native controls can be pasted unchanged into JavaScript.

## Status terminology

| Status | Meaning |
| --- | --- |
| Implemented | A concrete implementation of the listed JavaScript API exists. This does not certify every upstream overload or edge case. |
| Adapted | The capability is implemented using an explicit language/platform mapping. Call shape, runtime type behavior, or lifecycle can differ. |
| Partial | A useful portion exists, with a material upstream surface or behavior still missing. |
| Not provided | No corresponding implementation is delivered. |
| Native-specific | The API depends on a .NET/native facility; a web counterpart is listed where one exists. |

## MVVM model and observation

| Upstream API/family | ReactiveWeb mapping | Status and boundary |
| --- | --- | --- |
| `ReactiveObject`, changing/changed notifications | `ReactiveObject`, `Changing`, `Changed`, `PropertyChanging`, `PropertyChanged` | **Adapted.** Properties use JavaScript descriptors and RxJS streams. .NET event subscription syntax and CLR reflection are not used. |
| `RaiseAndSetIfChanged(ref field, value)` | `RaiseAndSetIfChanged(propertyName, value)`, `GetValue`, `SetValue` | **Adapted.** Values are held by name; JavaScript has no C# `ref` field arguments. Default equality is `Object.is`. |
| `[Reactive]`-like properties | `DefineReactiveProperty`, `defineReactiveProperties`, constructor initial values; generation entry point | **Implemented.** A normal class field is not automatically observable; use these accessors, schema/decorators, or explicit notification methods. |
| `RaisePropertyChanging`, `RaisePropertyChanged` | Same method names with optional old/new values | **Adapted.** Event records include sender, name, current/new value and previous value. |
| Suppress/delay notifications | `SuppressChangeNotifications`, `DelayChangeNotifications` | **Implemented.** Disposable nesting controls suppression and coalescing. Read the implementation for delivery order when combining scopes. |
| `ObservableForProperty` | Same function/instance name with `ObservePropertyOptions` | **Adapted.** Supports initial/before-change/distinct options, nested paths, and replaced reactive objects. Arbitrary uninstrumented object writes are not observable. |
| Property-observation providers | `IObservableForProperty`, `ObservablePropertyProviderRegistry`, `ObservablePropertyProviders` | **Adapted.** Explicit providers connect other property systems. Highest positive affinity wins, with later registration breaking ties. Providers emit change notifications, not an initial value. |
| `WhenAnyValue` | Same function/instance name; one or multiple paths plus a projector | **Adapted.** Strings, member selectors and path arrays replace compiled .NET expression trees. JavaScript variadic forms replace generated generic overload arities. |
| `WhenAny`, `WhenAnyDynamic` | Same names returning/projecting observed-change records | **Adapted.** Dynamic refers to JavaScript member paths, not a CLR `Expression` object. |
| `WhenAnyObservable` | Same name; switches observable properties, merges multiple sources or combines them through a projector | **Adapted.** Uses actual RxJS observables and operators. |
| `ObservedChange`, event argument interfaces | Structural `PropertyChangedEvent` / `IReactivePropertyChangedEventArgs` records | **Adapted.** JavaScript records replace runtime .NET event-argument classes. |
| `ObservableAsPropertyHelper`, `ToProperty` | Same names; initial value, callbacks, comparer, scheduler, deferred subscription, disposal | **Adapted.** `ToProperty` can install a read-only getter directly; .NET `out` helper arguments become return values. |
| `ReactiveProperty<T>`, `ToReactiveProperty` | Observable mutable value, value/source construction, `Create`, duplicate/initial-value options, refresh and validation | **Adapted.** Value/error observation and synchronous/Promise/observable validators are implemented. `AddValidationErrorObservable` explicitly identifies persistent stream-transform validators. Delivery is immediate by default; use `options.scheduler` for scheduled value observation. The audited upstream defaults to its task-pool scheduler. |
| Form validation helpers | `ReactiveValidationObject`, `ValidationContext`, `ValidationRule` | **Implemented** as additional web conveniences. These are not claimed to be every declaration in the separate ReactiveUI.Validation package. |
| Exception streams | `ThrownExceptions`, `ReportException`, `RxApp.HandleException` | **Adapted.** The default application handler logs errors. Configure a handler to implement application-specific escalation. Exceptions thrown by RxJS subscriber callbacks follow RxJS's own error-reporting behavior. |
| `ReactiveRecord` and CLR record equality | Use reactive object/schema classes | **Not provided** as C# record cloning, value equality, or compiler-generated record behavior. |

## Commands and interactions

| Upstream API/family | ReactiveWeb mapping | Status and boundary |
| --- | --- | --- |
| `ReactiveCommand.Create` | Same static factory | **Implemented.** Wraps a synchronous function. |
| `ReactiveCommand.CreateFromTask` | Same static factory with `(input, signal) => PromiseLike` | **Adapted.** Promise replaces Task; AbortSignal replaces CancellationToken. |
| `ReactiveCommand.CreateFromObservable` | Same static factory | **Implemented.** Source teardown participates in cancellation. |
| Command state | `CanExecute`, `IsExecuting`, `ThrownExceptions`, aggregate observable results; synchronous value getters | **Implemented.** Explicit `Execute` is independent of eligibility; `Invoke` and bindings gate invocation. |
| Command execution | `Execute(input)`, `.subscribe(...)`, `executeAsync(input)` | **Adapted.** Execute is cold and each subscription starts work. The Promise convenience resolves the final value. |
| Combined commands | `ReactiveCommand.CreateCombined` | **Adapted.** Child eligibility and aggregate results/errors are coordinated; refer to command tests for multi-emission and cancellation cases. |
| `InvokeCommand` | RxJS operator form and subscribed extension-function form | **Adapted.** Observable pipelines use `.pipe(InvokeCommand(command))`; no patching of RxJS prototypes is required. |
| Thread-pool execution | Async scheduling and application-provided Web Worker functions | **Partial.** An RxJS async scheduler delays work on the event loop; it does not move arbitrary function execution to a worker thread. |
| `ICommand`, `CanExecuteChanged` CLR event | Observable `CanExecute`, `Invoke`, HTML/React command adapters | **Native-specific.** No CLR interface or native event delegate exists. |
| `Interaction<TInput,TOutput>` | `Interaction`, `InteractionContext`, `RegisterHandler`, `Handle` | **Implemented.** Newest handler gets first refusal, handlers can decline, output is set once, and no result produces `UnhandledInteractionException`. |
| Sync/task/observable interaction overloads | Handler returns void, Promise or RxJS input; output through `context.SetOutput` | **Adapted.** A returned Promise is awaited for handler completion; its return value does not replace `SetOutput`. |
| Interaction lifetime | Disposable handler registration; request AbortSignal; `Dispose` | **Adapted.** Browser asynchronous work must cooperate with cancellation. Each Handle subscription creates a fresh handler snapshot/context; the audited upstream captures these at the Handle call. |

Unsubscribing from a Promise command requests cancellation through its signal. A non-cooperative Promise continues running; the command tracks it until settlement unless the command itself is disposed. The output scheduler controls aggregate command output and state delivery, while the observable returned by `Execute` forwards its source directly. Browser execution remains within JavaScript's event-loop model.

## Views, HTML and framework integration

| Upstream API/family | ReactiveWeb mapping | Status and boundary |
| --- | --- | --- |
| `IViewFor<T>`, ViewModel/DataContext | Structural `IViewFor`, `ReactiveElement`, `ReactiveUserControl` alias | **Adapted.** Native custom elements synchronize ViewModel and DataContext. |
| `WhenActivated`, `ViewModelActivator` | Same names, disposable activation scopes | **Implemented.** Reference-counted activation creates one resource scope and releases it on the final handle. |
| Visual-tree activation | Element connect/disconnect; framework effects | **Adapted.** CSS visibility changes do not by themselves detach a DOM element. |
| `Bind`, `OneWayBind`, `BindTo` | Functions in the `./html` entry point | **Adapted.** Explicit element/property/event arguments replace view-expression overloads and native binding engines. |
| Two-way text/input binding | Default input/change events, composition handling, conversion callbacks | **Implemented.** Events must actually be dispatched when code changes a DOM input programmatically. |
| `BindCommand` | DOM event binding, disabled/ARIA state, parameter values/functions | **Adapted.** Declarative bindings can follow a replaced command property. |
| `BindInteraction` | `BindInteraction(viewModel, path, handler)` in the HTML adapter | **Adapted.** Follows replaced interaction properties and removes the previous registration; dispose with the view activation scope. |
| `IReactiveBinding` | `ReactiveBinding` disposal scope | **Partial.** A disposable binding exists; the complete upstream expression metadata system is not equivalent. |
| Conversion system | `ConverterService`, typed/fallback/set-method registries, `BindingConverters` and callbacks | **Adapted.** Runtime tokens replace CLR types and a result record replaces `out` arguments. Defaults cover supported string, number, boolean and ISO UTC date conversions; the complete .NET nullable/numeric-width/date/culture converter class suite is not supplied. |
| Binding hooks | `IPropertyBindingHook`, `PropertyBindingHookRegistry` | **Adapted.** A hook can reject a binding before subscriptions/listeners are installed. This uses explicit JavaScript binding context rather than a CLR expression tree. |
| Declarative HTML | `BindHtml`, `data-rx-*` attributes, mutation observation | **Adapted.** Property-path-based HTML binding avoids runtime source evaluation; it is an additional browser API. |
| `ViewModelViewHost` | Custom-element host resolving an HTML view factory | **Adapted.** Supports VM assignment, contracts, default content and disposal. |
| `RoutedViewHost` | Custom-element host observing a routing VM stream | **Adapted.** DOM content replaces Avalonia transitioning content controls. |
| `ViewLocator`, contracts | Explicit constructor/factory registrations and inheritance lookup | **Adapted.** Singleton registrations own cached views; hosts detach and reuse them until registration disposal. Assembly scanning, generic CLR type resolution and reflection attributes are not used. |
| React integration | `useObservable`, `useReactiveObject`, `useWhenActivated`, `useReactiveCommand`, providers and view hosts in `./react` | **Adapted.** React is optional for the core and HTML packages. The command hook resolves the first result and unsubscribes; `executeAsync` on a core command resolves its final result. |
| Avalonia styled properties, XAML, native controls | DOM properties, custom elements and framework components | **Native-specific.** Avalonia layout/rendering, XAML loading, native windows and platform routed events are not ported. |
| WPF, WinForms, MAUI, UIKit, Android, Blazor native adapters | HTML/React counterparts | **Native-specific.** The existing native adapter packages are not executable in this JavaScript library. |

Use the core package in server code; construct/register custom elements only after a DOM is available. An imported element class uses the HTMLElement constructor present at module evaluation time. Importing that class before installing a DOM shim and later attempting to register it in a different realm is not a supported hydration mechanism.

## Application services, collections and persistence

| Upstream API/family | ReactiveWeb mapping | Status and boundary |
| --- | --- | --- |
| `RoutingState`, `IScreen`, `IRoutableViewModel` | Same core concepts and names | **Adapted.** JavaScript interfaces are structural; VMs identify a path segment and owning screen. |
| Navigate/back/reset | Reactive commands and `NavigationStack` | **Implemented.** NavigateBack eligibility requires at least two VMs. |
| Current VM and navigation lifetime | `CurrentViewModel`, value/path helpers, `WhenNavigatedTo`, `WhenNavigatedToObservable`, `WhenNavigatingFromObservable` | **Adapted.** Navigation callbacks can own a resource until departure. Browser URL routing is a separate concern; the VM stack is the core router. |
| Observable collections/change sets | `ObservableCollection`, immutable snapshots, mutation batches and `Connect` | **Adapted.** JavaScript array-based records replace .NET collection events. |
| Derived lists and DynamicData | `BindableDerivedList`, `DynamicData` namespace, `ToDynamicDataChangeSet`, `BindChangeSet`, `ToReactiveCollection` | **Integrated.** Uses the published DynamicDataWeb engine for cache/list pipelines and applies change sets to ReactiveWeb collections. Property observation, UI adapters and persistence share the same RxJS dependency. See [integration contracts](dynamic-data.md) and DynamicDataWeb’s separate overload/algorithm adaptations. |
| Ordered/chained comparers | `OrderedComparer.OrderBy`, `OrderByDescending`, `ThenBy`, `ThenByDescending`, `Compare` | **Adapted.** Uses JavaScript comparison semantics and can be supplied to native array sorting or derived lists. |
| `MessageBus` | Same named methods with explicit token and optional contract | **Adapted.** `Listen` observes future messages; `ListenIncludeLatest` replays the last published message, if any. Generic CLR message inference becomes an explicit JavaScript token. |
| Splat-style resolution | `ServiceLocator`, `Locator`, constants/factories/lazy singletons/contracts | **Adapted.** This is a JavaScript service container, not a port of every Splat integration package. |
| Application scheduling | `RxApp`, `RxSchedulers`, `RxState`, `RxSuspension` | **Adapted.** Uses RxJS queue/async schedulers by default. The modular facades configure the existing services, and injected schedulers support RxJS virtual-time testing. |
| `AutoPersist` | Debounced save function, serialized saves, error stream, explicit trigger/flush | **Adapted.** JavaScript serialization and ownership replace .NET serializer attributes. |
| Collection persistence | `AutoPersistCollection`, `ActOnEveryObject`, per-object save/cleanup | **Adapted.** Accepts ReactiveWeb collections and DynamicData sources/change sets; owns one resource per distinct live object. No arbitrary CLR metadata/serializer-attribute reader is provided. |
| `SuspensionHost`, setup extension | Lifecycle streams, app state, pluggable driver | **Adapted.** Driver methods can be synchronous, Promise-based or observable. |
| Browser suspension driver | `LocalStorageSuspensionDriver` / `BrowserSuspensionDriver` | **Implemented.** Explicit serialize/deserialize hooks support application migration/reconstruction. Storage failure is observable/catchable. |
| Browser lifecycle | `AttachBrowserLifecycle` | **Adapted.** Visibility/page events request persistence; browsers cannot guarantee completion of asynchronous shutdown work. |
| App instance/builder/provider suite | `RxAppBuilder`, `DefaultRxAppBuilder`, `ReactiveUIBuilder` / `ReactiveWebBuilder` aliases, `ReactiveApplication` | **Adapted.** Fluent setup configures services, views, messages, property providers, suspension and schedulers; built applications own setup lifetimes. CLR assembly scanning, external DI integrations and every upstream fluent overload are not reproduced. |
| `ScheduledSubject<T>` | Same named observable/observer wrapper | **Adapted.** Delivers notifications through an RxJS scheduler and uses the optional default observer when no ordinary observer is subscribed. |
| `WaitForDispatcherScheduler` | Same named RxJS scheduler wrapper | **Adapted.** Retries obtaining the configured scheduler with a fallback; browser options additionally support waiting for readiness. Native dispatcher/thread identity is not reproduced. |
| Logging/switching mixins | `Log`, `LoggedCatch`, `Do`, `SwitchSelect`, `SwitchSubscribe` and mixin objects | **Adapted.** Supports function/operator-style RxJS composition. Null outer values preserve the current inner subscription, matching the inspected upstream switch helper. Default LoggedCatch emits `undefined` as the JavaScript default-value adaptation. |
| `RxCacheSize`, internal reflection/expression caches | No equivalent public cache-tuning surface | **Not provided.** The JavaScript implementation does not use the upstream reflection/compiled-expression cache engine. |

Persistence is JSON-based by default. Applications must explicitly reconstruct prototypes, commands and subscriptions, handle schema versions, and choose storage appropriate to their data. A snapshot of a VM is not automatically a serialized running observable graph. Storage survives only according to the browser/user's persistence behavior; localStorage is not a remote synchronization or collaboration server.

## Property generation

| Upstream generator | ReactiveWeb mapping | Status and boundary |
| --- | --- | --- |
| `[Reactive]` | `@Reactive accessor`, `reactiveProperty`, `defineViewModel` | **Adapted.** Standard TypeScript auto-accessor decorators require compilation; generated/runtime schema usage also works without decorators. |
| Dependent notification | Property `dependents` option | **Implemented.** A property can invalidate named synchronous computed getters. |
| Setter validation/equality | `validate` and `equals` schema/decorator options | **Implemented** as JavaScript conveniences. |
| `[ObservableAsProperty]` | Schema `computed` source definitions and `ToProperty` | **Adapted.** Owns observable helper subscriptions and exposes cached getters. |
| `[ReactiveCommand]` | Schema `commands` with sync/task/observable kinds | **Adapted.** Commands, permission streams and output schedulers are created per VM instance. |
| Build-time source generation | `reactiveweb-generate` CLI | **Adapted.** Produces JavaScript-oriented generated source from a supported schema. It does not compile arbitrary C# code. |
| `[IReactiveObject]`, partial classes | Base class/schema factory | **Adapted.** No Roslyn partial-class augmentation or C# accessibility enforcement. |
| `[IViewFor]`, generated native hosts | Explicit HTML view classes/hosts and view registration | **Adapted.** No generated Avalonia/WinForms/XAML source. |
| Roslyn diagnostics/analyzers/suppressors | TypeScript validation and schema diagnostics | **Partial.** The upstream Roslyn diagnostic suite and every attribute option are not reproduced. |

Generated classes use standard property accessors rather than polling or a Proxy on each property read/write. Selector-to-path discovery uses a Proxy when a selector is parsed. Performance claims should be based on the included benchmark in the target browser/runtime, with realistic subscriber and view counts.

## .NET-to-JavaScript call-shape rules

| .NET pattern | JavaScript/TypeScript pattern |
| --- | --- |
| `IObservable<T>` / `System.Reactive` operators | RxJS `Observable<T>` and `.pipe(...)` operators |
| `IDisposable.Dispose()` | Returned ReactiveWeb `Dispose()` or RxJS `unsubscribe()`; `DisposeWith` accepts either |
| `Unit` / parameterless command | `void` / `undefined` by default |
| `Task<T>` / `CancellationToken` | `PromiseLike<T>` / `AbortSignal` |
| `Expression<Func<T,V>>` | Supported single-member selector or explicit path; general functions are not expression trees |
| Extension methods | Exported functions and selected instance conveniences |
| Generic type identity (`Listen<T>`, `GetService<T>`) | Explicit constructor, symbol, or string token |
| Overload resolution | TypeScript overloads, options objects, variadic arguments and explicit factories |
| `ref` / `out` arguments | Named property storage and returned helper/result values |
| CLR numeric and nullable types | JavaScript number/bigint/null/undefined with application-specific conversion rules |
| Native control lifetime | DOM connection or framework mount lifetime |

JavaScript equality, object identity, task cancellation and scheduling differ from .NET. Passing a value through TypeScript's compile-time type system does not create a CLR runtime type or enforce .NET generic variance.

## Verification and remaining work

The repository includes tests for the JavaScript contract and integration checks for shipped adapters. Use the README's validation commands to run the current suite. Successful local tests establish the covered behaviors in those runtimes; they do not establish exhaustive upstream conformance or every browser/framework combination.

| Contract area | Executable evidence |
| --- | --- |
| Notifications, nested observation, OAPH and disposable ownership | [core.test.ts](../test/core.test.ts) |
| Cold commands, overlap, errors, cancellation, scheduling and combined results | [command.test.ts](../test/command.test.ts) |
| Interaction priority, refusal, async completion, snapshots and cancellation | [interaction.test.ts](../test/interaction.test.ts) |
| Activation, services, collections, message bus, routing and storage | [services.test.ts](../test/services.test.ts) |
| DOM binding, composition, command lifetime, mutation, view/interaction rebinding | [html.test.ts](../test/html.test.ts) |
| React external-store subscriptions, component lifetime and command hooks | [react.test.ts](../test/react.test.ts) |
| Validation errors, pending work and obsolete-result cancellation | [validation.test.ts](../test/validation.test.ts) |
| Runtime/decorator generation, disposal and generated TypeScript/CLI execution | [generation.test.ts](../test/generation.test.ts) |
| Application setup, default restoration, property providers and converter registration | [builder.test.ts](../test/builder.test.ts) |
| Typed/fallback/set-method conversion, affinity selection and binding hooks | [converters.test.ts](../test/converters.test.ts) |
| Scheduled subjects, dispatcher fallback/readiness, logging and switching helpers | [extensions.test.ts](../test/extensions.test.ts) |

Remaining broad compatibility work includes a complete declaration-level mapping across upstream target frameworks, differential upstream behavioral fixtures, upstream conversion/provider/builder overloads beyond the documented JavaScript registries, Roslyn diagnostic equivalence, and native-only facilities that require explicit web alternatives. Production use also needs application-specific browser, accessibility, lifecycle/memory, storage and workload performance verification.

Do not calculate a parity percentage from this table: families vary substantially in size, and an implemented name is not proof of matching all overloads and semantics. Add a test and update this matrix whenever a previously partial behavior becomes supported.
