# ReactiveWeb.Blazor

A .NET 8 / .NET 10 Razor Class Library exposing the actual ReactiveWeb/RxJS browser implementation through typed reactive services, value and command components, and complete native interop.

```sh
dotnet add package ReactiveWeb.Blazor --version 0.3.0
```

## Reactive models and Razor bindings

`ReactiveProvider` owns a per-component browser session and exposes it through `Ready` and `RenderFragment<BrowserModule>`. `ReactiveModel` supports native GetValue/SetValue, delayed batch updates, property-path observation and compact change notifications. `ReactiveValue<TValue>` subscribes to a property path and renders a `RenderFragment<TValue>` on the Blazor renderer, automatically detaching when its model/path changes or the component unmounts.

```csharp
// Initialize from Ready/OnAfterRenderAsync, not static prerender.
model = await ReactiveModel.CreateAsync(module, new { count = 1 });
subscription = await model.ObserveAsync<int>("count", value =>
    InvokeAsync(() => { count = value; StateHasChanged(); }));
await model.SetAsync("count", 2);
```

```razor
<ReactiveValue TValue="int" Model="model" Property="count">
    <ChildContent Context="value"><strong>@value</strong></ChildContent>
</ReactiveValue>
```

Property names match serialized JavaScript names, normally camelCase. The native ObservableForProperty/WhenAnyValue logic handles nested path subscriptions; native reference identity remains available through `Handle`. Observer values use an explicit envelope so a legitimate value containing `error` or `completed` properties is not confused with an Rx notification. Property-change notifications omit Sender's live subscriber/model graph.

## Commands and cancellation

`BrowserCommand<TInput,TOutput>` wraps a real native ReactiveCommand. It supports synchronous browser or asynchronous task callbacks, execution results, CanExecute and IsExecuting state, enabling/disabling, result/error streams, cancellation and disposal. The task-oriented ExecuteAsync honors CanExecute; access `Handle.Command` through native interop for the underlying engine's explicit execution semantics.

```csharp
reference = DotNetObjectReference.Create(this);
command = await BrowserCommand<int, int>.CreateAsync(module,
    BrowserFunction.DotNet(reference, nameof(Increment)));
// [JSInvokable] public Task<int> Increment(int value) => Task.FromResult(value + 1);
var result = await command.ExecuteAsync(10, cancellationToken);
```

`ReactiveCommandButton<TInput,TOutput>` binds enabled/busy state to the native command, executes the supplied `Parameter`, and invokes `Executed` or `Error`. It does not own the command. A cancelled ExecuteAsync wait requests native cancellation; CancelAsync and disposal unsubscribe active executions. Native task AbortSignals are passed to a JavaScript module callback only when `passSignal` is true. Asynchronous .NET callbacks receive the input only and must implement their own server-side cooperative cancellation when needed; aborting the browser does not magically stop arbitrary .NET work.

Use `BrowserFunction.Module("./commands.js", "execute")` for browser callbacks. Property/Setter/Constant descriptors are synchronous and require no eval. DotNet descriptors return promises and cannot replace synchronous selectors/comparers. Command failures reject the returned task and remain observable through the native error stream rather than becoming unhandled global errors.

## Complete native API

`ReactiveModule` inherits BrowserModule with `GetExportsAsync`, `CreateAsync`, `InvokeAsync`, `CallAsync`, `GetAsync`, `SetAsync`, `SubscribeAsync` and `ReleaseAsync`. The root ReactiveWeb exports include routing, activation, interactions, validation, collections, persistence, generation, converters and DynamicData integration; `Html`, `Rx`, and `RxOperators` namespaces are also bundled. Generic native interop covers APIs beyond the typed convenience layer; this is not an exhaustive generated C# reimplementation of ReactiveUI.

Use `IJSObjectReference` for native models, observables and commands, DTOs/JsonElement for value data, and retain subscriptions for their intended lifetime. Dispose subscriptions, commands and models before their provider. The provider cleans remaining session-owned resources and .NET callback references; disconnected Server circuits are tolerated. A module belongs to a component/circuit, never a Server-wide singleton. Interop-handle disposal alone is not native disposal: `ReleaseAsync` does both. JSON-transferred data does not become a shared CLR object graph.

Generic event snapshots are bounded, omit private native backing graphs, and may contain `$reference` or `$truncated` markers. Read explicit data or native references instead of treating an event snapshot as a full model serialization. Large Server payloads also require an appropriately configured SignalR receive limit and application-level batching.

## Hosting, builds and samples

Consumers need no npm, Node, CDN or Dockyard NuGet dependency. The package contains the engine, RxJS and transitive browser code as static web assets. Use interactive WebAssembly or Server; prerender invokes no JavaScript. Assets resolve through the application base URI under `_content/ReactiveWeb.Blazor`.

```sh
git submodule update --init --recursive
npm ci
npm run build
node blazor/build.mjs
dotnet run --project blazor/sample/Sample.csproj
dotnet run --project blazor/server/Server.csproj --urls http://localhost:5080
# Server: http://localhost:5080/probe/
```

The common runtime, project/host templates and tests are generated from a commit-pinned source submodule. Typed services, Razor components, adapter and sample are ordinary reviewed files in this repository. Generated files are ignored and recreated before packing; the resulting NuGet package is self-contained.

CI compiles/packs both frameworks, checks nupkg contents, runs bridge and managed lifecycle/prerender tests, then restores and drives actual-package WebAssembly/Server consumers in Chromium. The sample executes a native ReactiveCommand through an asynchronous .NET callback, observes a real native property update, verifies CanExecute and repeatedly unmounts/remounts. Packages, published samples and browser diagnostics are retained as artifacts.

## NuGet releases

`blazor/Version.props` versions NuGet independently of npm. A validated version-changing main merge publishes using `NUGET_API_KEY`, falling back to `NUGET_TOKEN` or `NUGET_KEY`. Manual dispatch defaults to validation-only. Releases use `blazor-v<version>` and attach packages and the runnable WebAssembly sample without altering npm release tags. Update the source submodule and reusable workflow SHA together through reviewed PRs. Existing native-engine compatibility limits remain applicable.
