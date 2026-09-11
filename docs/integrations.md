# HTML, Web Components, React and validation

ReactiveWeb uses RxJS observables for change notifications, commands, validation and navigation. The core has no DOM or React runtime dependency. Import HTML adapters from `@wieslawsoltes/reactiveweb/html` and React adapters from `@wieslawsoltes/reactiveweb/react`. React is an optional peer dependency.

## Programmatic HTML binding

```ts
import { ReactiveObject, CompositeDisposable, ReactiveCommand } from '@wieslawsoltes/reactiveweb';
import { Bind, OneWayBind, BindCommand, BindingConverters } from '@wieslawsoltes/reactiveweb/html';

const vm = new ReactiveObject({ Name: 'Ada', Age: 30 });
const lifetime = new CompositeDisposable();
lifetime.Add(Bind(vm, 'Name', document.querySelector('#name')!));
lifetime.Add(Bind(vm, 'Age', document.querySelector('#age')!, 'value', {
  converter: BindingConverters.Number,
  onError: error => console.error(error),
}));
lifetime.Add(OneWayBind(vm, 'Name', document.querySelector('#greeting')!, 'textContent', {
  convert: name => `Hello ${name}`,
}));
const save = ReactiveCommand.Create(() => console.log(vm.GetValue('Name')));
lifetime.Add(BindCommand(save, document.querySelector('#save')!));
// Tear down when the screen closes:
lifetime.Dispose();
save.Dispose();
vm.Dispose();
```

`Bind(viewModel, path, element, property?, options?)` initializes from the view model, then observes both directions. The default property is `checked` for checkboxes/radios, `value` for text inputs/selects/textareas, and `textContent` otherwise. The default event is `change` for selects/checked and `input` otherwise; `options.event` selects another event. IME composition is committed at `compositionend`. Nested dotted property paths follow reactive intermediate-object replacement.

`OneWayBind` only observes model changes. `BindTo(observable, element, property?, options?)` accepts any RxJS observable. A property can also be `attr.title`, `attr.aria-label` or `class.selected`. Event-handler properties, HTML insertion and executable attribute/URL values are rejected. Render trusted HTML through normal DOM construction. The declarative path grammar intentionally supports identifiers separated by dots, rather than expressions or arbitrary code.

Direct converters expose `Convert` and optional `ConvertBack`; inline `convert`/`convertBack` functions are also accepted. Built-ins are `String`, `Number`, `Boolean` and `Not`. The direct Number converter uses JavaScript finite numbers; empty input maps to `null`, and invalid input reports an error while retaining the last model value.

The separate `ConverterService` provides .NET-style typed, fallback and set-method converter registries. `BindingTypeConverter(fromToken, toToken, callback, affinity)` implements `FromType`, `ToType`, `GetAffinityForObjects()`, `TryConvertTyped` and `TryConvert`. JavaScript `out` parameters become `{ Success, Result }` objects, also available as `{ success, value }`. Use `Conversion.Success(value)` or `Conversion.Failure(error)` in callbacks. Registries choose the highest positive affinity, with the latest registration winning ties; an exact typed converter takes precedence over fallback converters. Registrations return disposable handles so temporary overrides can be reverted.

```ts
import { ConverterService, BindingTypeConverter, Conversion } from '@wieslawsoltes/reactiveweb';
const converters = new ConverterService();
const currency = Symbol('currency');
const registration = converters.TypedConverters.Register(
  new BindingTypeConverter(currency, String, amount => Conversion.Success(`€${amount}`))
);
// Explicit tokens preserve type intent across JavaScript's erased generic types.
Bind(vm, 'Age', ageInput, 'value', {
  conversionService: converters, sourceType: Number, targetType: String,
});
registration.Dispose();
```

`ConverterService.Current` and its `RxConverters.Services` alias supply the default service. Built-in registry conversions handle finite decimal numbers, strict `true`/`false`/`1`/`0` booleans, primitive strings and ISO UTC dates; invalid calendar dates are rejected. Unlike the direct optional-number converter, registry String→Number rejects empty input. Native .NET integer widths and culture-specific formats require custom converters. Set-method converters are resolved explicitly using `ResolveSetMethodConverter` and `PerformSet` for collection-style assignments. `PropertyBindingHookRegistry` supports provider vetoes before subscribing; pass it through `bindingHooks` or register against its `Current` instance. A rejected binding reports `IsBound === false`.

Bindings report errors through `options.onError` or a bubbling, composed `reactive-error` custom event. Bindings are disposable and also accept `unsubscribe()`. Always place manually created bindings in an activation scope or another owned lifetime.

## Declarative views

```html
<input data-rx-bind="Name">
<input type="number" data-rx-bind="Age" data-rx-converter="Number">
<span data-rx-text="Name"></span>
<button data-rx-command="Save" data-rx-parameter="DocumentId">Save</button>
<section data-rx-visible="HasSelection">Selection details</section>
<input data-rx-bind="Email">
<p data-rx-validation="Email"></p>
```

Call `BindHtml(root, viewModel)` once. It watches additions, removals and binding-attribute changes through `MutationObserver`, disposing bindings on removed nodes and replacing changed bindings. Nested view elements exposing `ViewModel` keep their own binding scope; a `data-rx-scope` attribute establishes the same boundary for manually bound containers. `observeMutations: false` disables this behavior for fully static templates. Custom converter names are supplied through `{ converters: { name: converter } }`.

| Attribute | Behavior |
| --- | --- |
| `data-rx-bind`, `data-rx-value` | Two-way default-property binding |
| `data-rx-checked` | Two-way checked state |
| `data-rx-text` | Text-only rendering |
| `data-rx-one-way` | One-way default-property binding |
| `data-rx-visible` | Inverts the value into `hidden` |
| `data-rx-enabled` | Inverts the value into `disabled` |
| `data-rx-command` | Observes/rebinds the command and applies `CanExecute` |
| `data-rx-parameter` | Reads the named property when the event occurs |
| `data-rx-event` | Overrides the input/command event |
| `data-rx-converter` | Resolves a named converter |
| `data-rx-validation` | Displays errors for the property, or all errors when empty |

Declarative bindings never evaluate expressions or compile strings. Use computed observable properties for formatting and conditions. Templates are trusted application structure; model strings are rendered as text.

## Web Components and activation

```ts
import { ReactiveElement } from '@wieslawsoltes/reactiveweb/html';
import { EditorViewModel } from './editor-view-model.js';

class EditorView extends ReactiveElement<EditorViewModel> {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' }).innerHTML = `
      <input data-rx-bind="Name">
      <button data-rx-command="Save">Save</button>
    `;
    this.ViewModel = new EditorViewModel();
    this.WhenActivated(disposables => {
      disposables.Add(this.ViewModel!.Changed.subscribe(console.log));
    });
  }
  override Dispose() {
    const ownedViewModel = this.ViewModel;
    super.Dispose();
    ownedViewModel?.Dispose();
  }
}
customElements.define('editor-view', EditorView);
```

`ReactiveElement<T>` implements `IViewFor<T>`, exposes `ViewModel` and its `DataContext` alias, and binds its shadow root or light-DOM contents on connection. Disconnection disposes the bindings and releases activation leases. Reconnection creates fresh resources. Replacing `ViewModel` while connected recreates the view activation scope and releases the old view model's activation lease. A model with an `Activator` property participates in reference-counted activation. The view does not own/dispose the model itself; shared models remain usable by other views.

The HTML entry point can be imported during server rendering without an `HTMLElement` global. Instantiate/register components in a browser context. A server-loaded class must not be reused after retrofitting a DOM into the same runtime; load the browser module in the browser realm. `RegisterReactiveElements()` explicitly registers `reactive-view`, `reactive-view-host` and `reactive-routed-view-host`.

Use `BindInteraction(vm, 'ConfirmDelete', context => ...)` in a view activation scope. It disposes old handler registrations if the interaction property is replaced. Handlers receive `Input`, `Signal`, `IsHandled` and `SetOutput`; asynchronous dialogs can return a promise or observable. A cancelled request signals `context.Signal`, allowing the view to close its dialog.

## Routing and view location

`ViewModelViewHost` renders its `ViewModel`. `RoutedViewHost` observes its `Router.CurrentViewModel`. Both use `ViewLocator`, `ViewContract` and `DefaultContent`; changing the locator, contract or router while connected replaces subscriptions and views. Missing registrations or null models display the fallback. HTML factories return a DOM node or `{ Element, ViewModel }`; view-model-aware results receive their `ViewModel`/`DataContext`, and disposable transient view results are disposed on replacement/detachment. Singleton registrations retain their view across navigation and own its final disposal.

```ts
import { ViewLocator } from '@wieslawsoltes/reactiveweb';
import { RoutedViewHost } from '@wieslawsoltes/reactiveweb/html';
ViewLocator.Current.Register(EditorViewModel, vm => {
  const view = document.createElement('editor-view') as EditorView;
  view.ViewModel = vm;
  return view;
});
// A registered routed-view-host can receive its Router property directly.
document.querySelector('reactive-routed-view-host')!.Router = screen.Router;
```

## Reactive validation

```ts
import { ReactiveValidationObject, ValidationRule } from '@wieslawsoltes/reactiveweb';
import { BindValidation } from '@wieslawsoltes/reactiveweb/html';
const vm = new ReactiveValidationObject({ Email: '' });
ValidationRule<string>(vm, 'Email', value => value.includes('@'), 'Enter an email address.');
ValidationRule<string>(vm, 'Email', async value => {
  const response = await fetch(`/api/available?email=${encodeURIComponent(value)}`);
  if (!response.ok) throw new Error('Validation request failed');
  return (await response.json()).available;
}, 'This email is unavailable.');
const binding = BindValidation(vm, document.querySelector('#errors')!, 'Email');
```

Rules can return a boolean, a string (empty means valid), an array of errors, a `ValidationState`, a promise or an observable of those results. `ValidationContext.IsValid`, `IsPending`, `Text` and `ValidationStatusChange` are observables; `State`, `IsValidValue` and `IsPendingValue` are synchronous snapshots. `GetErrors(property?)` and `ObserveErrors(property?)` expose property-level errors. Pending validation is invalid, so `ValidationContext.IsValid` can gate a submit command.

Changing a value unsubscribes the old observable validator and ignores stale promise results. Promises themselves cannot be cancelled; use an observable with an abort teardown to cancel network work. Validator failures map to the rule's configured error message and subsequent values can recover. Disposing a rule removes it from aggregation; disposing the context disposes all owned rules. `PropertyValidationRule` also accepts an arbitrary values observable for multi-property or server-side conditions.

## React

```tsx
import { ReactiveProvider, useViewModel, useReactiveObject, useReactiveCommand,
  useWhenActivated } from '@wieslawsoltes/reactiveweb/react';

function Editor() {
  const vm = useReactiveObject(useViewModel<EditorViewModel>());
  const save = useReactiveCommand(vm.Save);
  useWhenActivated(vm);
  return <>
    <input value={vm.Name} onChange={event => { vm.Name = event.target.value; }} />
    <button disabled={!save.canExecute} onClick={() => {
      void save.execute(undefined).catch(vm.ReportError);
    }}>{save.isExecuting ? 'Saving…' : 'Save'}</button>
  </>;
}
// <ReactiveProvider viewModel={viewModel}><Editor /></ReactiveProvider>
```

`useReactiveObject(vm, selector?)` uses `useSyncExternalStore`, shares one active change subscription among consumers of a model, and releases it when the last consumer unmounts. It rerenders on any model `Changed` notification; a selector transforms the return value but does not implement selector-specific subscription filtering. Nested models need their own hook or an explicitly observed computed stream.

`useObservable(source, initialValue?, serverValue?)` caches the latest snapshot and subscribes after commit. Pass a stable observable (a stored field or `useMemo`) to avoid unnecessary resubscription. A `BehaviorSubject` can supply its current value without subscribing during render. Server rendering uses the explicit server snapshot or initial value without starting observable work. Serialized server/client initial values must match for hydration. Observable errors are thrown through the render path for a React error boundary.

`useWhenActivated(vm, block?, dependencies?)` owns activation and callback resources in an effect, including React StrictMode's setup/cleanup cycle. Dependencies recreate that scope. `useReactiveCommand` returns `canExecute`, `isExecuting`, `execute` plus PascalCase aliases. Its promise resolves the first result and unsubscribes; use `command.Execute(...).subscribe(...)` directly for multi-result commands. The hook gates invocation on current `CanExecute`, whereas the core command's explicit `Execute` deliberately bypasses UI gating.

`ReactiveProvider`/`useViewModel` and `createReactiveContext<T>()` support dependency injection through React context. React `ViewModelViewHost` and `RoutedViewHost` use the same view locator abstraction. Register a pure factory returning a component type, such as `locator.Register(EditorViewModel, () => EditorView)`, and pass `viewLocator` plus optional `contract`, `fallback` and `initialViewModel`. Components receive both `viewModel` and `ViewModel` props and a reactive context provider. Use a separate locator or contract when the same model supports both DOM and React views. React factories run during rendering and must be pure.

## Upstream references and browser adaptations

The design follows [ReactiveUI data binding](https://www.reactiveui.net/documentation/handbook/data-binding/), [activation](https://www.reactiveui.net/documentation/handbook/when-activated/), [command binding](https://www.reactiveui.net/documentation/handbook/commands/binding-commands/), [validation](https://www.reactiveui.net/documentation/handbook/user-input-validation/) and React's [`useSyncExternalStore` contract](https://react.dev/reference/react/useSyncExternalStore). This is an independent browser implementation. HTML events, custom-element lifecycle and React effects replace native visual-tree/property systems. It does not load Avalonia/XAML views, native binding hooks, .NET expression trees or the native converter/validation plugin binaries.
