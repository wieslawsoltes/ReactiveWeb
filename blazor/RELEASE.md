# ReactiveWeb.Blazor 0.3.1

Updates the pinned interop runtime to tested Dockyard revision `1c895b7184451071e1c7131063249d2d9eb145b9`, with no Dockyard runtime dependency.

- Await concurrent native/module/subscription cleanup and asynchronous unsubscribe, continuing teardown after individual failures.
- Preserve cyclic/deep argument graphs, shared callback identity and callable property/method/disposal access.
- Cancel initialization waits independently without cancelling shared initialization; prevent late work after owner disposal.
- Add `CallFunctionJsonAsync<T>` for complete streamed callable results and expanded JavaScript/managed regressions.

Typed reactive models, asynchronous native commands, Razor value/command components, complete DTO notifications and all native APIs remain available. Both .NET 8/.NET 10 WebAssembly and Server package consumers validate real native commands, property changes, CanExecute, streaming and Razor callbacks before publication. Public NuGet payloads are verified before creating the release. Native compatibility and asynchronous callback constraints are unchanged.
