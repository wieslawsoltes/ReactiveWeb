# ReactiveWeb

RxJS MVVM for JavaScript, HTML, React and Blazor with ReactiveUI-style .NET APIs.

[![npm](https://img.shields.io/npm/v/%40wieslawsoltes%2Freactiveweb)](https://www.npmjs.com/package/@wieslawsoltes/reactiveweb)
[![npm downloads](https://img.shields.io/npm/dm/%40wieslawsoltes%2Freactiveweb)](https://www.npmjs.com/package/@wieslawsoltes/reactiveweb)
[![ReactiveWeb.Blazor on NuGet](https://img.shields.io/nuget/v/ReactiveWeb.Blazor?label=ReactiveWeb.Blazor&logo=nuget)](https://www.nuget.org/packages/ReactiveWeb.Blazor)
[![NuGet downloads](https://img.shields.io/nuget/dt/ReactiveWeb.Blazor)](https://www.nuget.org/packages/ReactiveWeb.Blazor)
[![Blazor CI](https://github.com/wieslawsoltes/ReactiveWeb/actions/workflows/blazor.yml/badge.svg)](https://github.com/wieslawsoltes/ReactiveWeb/actions/workflows/blazor.yml)

## JavaScript

```sh
npm install @wieslawsoltes/reactiveweb rxjs
```

The [complete JavaScript guide](README.web.md) preserves the existing API examples, architecture, tests and compatibility/license information. [Open the web demo](https://wieslawsoltes.github.io/ReactiveWeb/).

## Blazor

```sh
dotnet add package ReactiveWeb.Blazor --version 0.3.2
```

The .NET 8/.NET 10 package supports interactive WebAssembly and Server. It includes the native engine and RxJS as local static web assets, typed reactive models and asynchronous commands, `ReactiveProvider`, `ReactiveValue<TValue>` and `ReactiveCommandButton<TInput,TOutput>`. Native object/function handles expose advanced operators. Consumers need neither npm nor a CDN.

Read the [Blazor guide](blazor/README.md), [integration contract](blazor/INTEGRATION.md), [working sample](blazor/sample/Demo.razor) and [release notes](blazor/RELEASE.md).

```sh
git submodule update --init --recursive
npm ci
npm run build
node blazor/build.mjs
dotnet run --project blazor/sample/Sample.csproj
# Or: dotnet run --project blazor/server/Server.csproj
```

Source builds require the .NET 10 SDK with .NET 8 targeting support. The Server sample uses `/probe/`. CI tests package-restored consumers for both frameworks and hosts, including native commands, reactive notifications, Razor callbacks, streamed data and remounting.

NuGet is independently versioned in `blazor/Version.props`. Version-changing main merges publish after validation with `NUGET_API_KEY` (`NUGET_TOKEN`/`NUGET_KEY` aliases), verify the downloaded public payload and create `blazor-v*` releases with packages, symbols, sample archives and checksums. Native engine and browser boundaries remain; this is not an exhaustive generated C# ReactiveUI port. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
