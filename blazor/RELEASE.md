# ReactiveWeb.Blazor 0.3.0

Add .NET 8 / .NET 10 reactive-model services, task-oriented native ReactiveCommand wrappers, cancellation/state/result observation, ReactiveProvider, Razor-templated ReactiveValue and ReactiveCommandButton. Bundle the full native engine, HTML adapter and RxJS exports as local assets. Include functional WebAssembly and Server samples exercising an asynchronous .NET callback, actual reactive notification, CanExecute and lifecycle cleanup, with actual-package CI and validated NuGet/release publishing.

Synchronous native callbacks stay in JavaScript; asynchronous .NET work requires its own cooperative cancellation beyond browser subscription cancellation.
