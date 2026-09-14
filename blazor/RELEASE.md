# ReactiveWeb.Blazor 0.3.2

Adopts merged and validated shared runtime c833be49d472583b6f56225862e0aa7d201c1da7 from Dockyard PR #5. Fixes concurrent visual cleanup, late template imports/root creation, queued callbacks after removal and retained failures. Adds lifecycle state, awaitable Razor factory disposal and coalesced parameter updates.

Typed reactive models, asynchronous commands, full DTO notifications, Razor value/command components and native operator access remain intact. Root and package READMEs and release notes identify 0.3.2. The shared source introduces no Dockyard runtime dependency.

Both .NET 8/.NET 10 actual-package WebAssembly/Server matrices run new managed visual/template lifecycle regressions and template movement/update/recreation, alongside native command/reactive checks and eight new shared JavaScript cases. Publication verifies the public NuGet payload before releasing packages, symbols and runnable samples. Synchronous native callbacks remain browser functions; engine boundaries are unchanged.
