# ReactiveWeb.Blazor

Install `ReactiveWeb.Blazor` 0.3.2 for .NET 8/.NET 10. The actual engine, HTML adapter, RxJS and browser dependencies are packaged as local static web assets for interactive WebAssembly and Server.

## Typed model and command binding

Create `ReactiveModel` from a provider's initialized `BrowserModule`, then use `GetAsync<T>`, `SetAsync`, `SetManyAsync`, `ObserveAsync<T>` or `ObserveChangesAsync`. DTO values and notifications use complete JSON streaming. Typed application values are literal data, not executable callback descriptors.

```razor
<ReactiveValue TValue="int" Model="model" Property="count">
    <ChildContent Context="value"><p>Count: @value</p></ChildContent>
</ReactiveValue>
<ReactiveCommandButton TInput="int" TOutput="int" Command="command"
                      Parameter="count" Executed="ApplyResult">
    Increment
</ReactiveCommandButton>
```

`ReactiveProvider` owns the browser session; construct models/commands after its `Ready`. `BrowserCommand<TInput,TOutput>` supports execution, CanExecute, IsExecuting, enabling, results/errors and cancellation. Use `BrowserFunction.DotNet` only for asynchronous native command APIs. The [sample](sample/Demo.razor) executes a real native command through an asynchronous .NET callback and checks property notifications and CanExecute gating.

Native APIs and returned operator functions remain available through `BrowserModule` and its reference APIs. Read [INTEGRATION.md](INTEGRATION.md) for hosting, templates, streaming and ownership. This wraps the browser engine with typed helpers rather than claiming an exhaustive generated C# ReactiveUI implementation.

## Lifecycle in 0.3.2

The shared runtime now serializes visual/template teardown, suppresses callbacks queued before removal, cleans up late imports/creation and retains cleanup failures for repeated callers. Native Razor factories have awaitable disposal, coalesced updates and state retention during synchronous movement. Managed and JavaScript regressions and actual-package movement/update/recreation tests run in both sample hosts alongside native command/reactive checks.
