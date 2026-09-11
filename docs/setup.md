# Application setup and modular defaults

`RxAppBuilder` constructs real service, view, messaging, persistence, property-provider, and binding-converter registrations. `Build()` installs those resources as the application defaults. `BuildApp()`, `DefaultRxAppBuilder`, `ReactiveWebBuilder`, and `ReactiveUIBuilder` provide familiar naming choices for application ports.

```ts
import {
  RxAppBuilder, LocalStorageSuspensionDriver,
  ReactiveObject, defineReactiveProperties,
} from '@wieslawsoltes/reactiveweb';
import { animationFrameScheduler, asyncScheduler } from 'rxjs';

class SettingsViewModel extends ReactiveObject {
  declare Theme: string;
  constructor() {
    super();
    defineReactiveProperties(this, { Theme: 'light' });
  }
}

const settings = new SettingsViewModel();
const app = RxAppBuilder.Create()
  .WithMainThreadScheduler(animationFrameScheduler)
  .WithTaskPoolScheduler(asyncScheduler)
  .WithExceptionHandler(error => console.error(error))
  .RegisterConstantViewModel(settings)
  .RegisterView(SettingsViewModel, viewModel => {
    const element = document.createElement('section');
    element.textContent = `Theme: ${viewModel.Theme}`;
    return element;
  })
  .WithSuspension(
    () => ({ selectedPage: 'home' }),
    new LocalStorageSuspensionDriver({ key: 'my-app.state' }),
  )
  .Build();

app.SuspensionHost.IsResuming.next();
const view = app.Views.ResolveView(settings);
document.body.append(view);

// On teardown: release bootstrap subscriptions and owned application resources.
app.Dispose();
```

Factories execute according to their registration lifetime. Singleton views remain reusable when a view host temporarily detaches them, and are disposed when their registration or application is disposed. `RegisterViewModel` and `WithService(token, factory)` create transient instances. `RegisterSingletonViewModel`, `RegisterSingletonView`, and `WithService(token, factory, { lifetime: 'singleton' })` create an instance lazily and reuse it. `WithInstance(instance, token)` and `RegisterConstantViewModel(instance)` register existing values. Explicit constructors, symbols, and strings replace erased CLR generic types and assembly scanning. Contracts can be supplied for multiple named registrations of the same type.

`WithServices`/`WithRegistration` and `WithViews`/`ConfigureViewLocator` receive the actual registries while the application is being built. `WithRegistrationOnBuild` receives the built application after global installation; return a disposable, an RxJS subscription, or a teardown function to attach work to the application lifetime. Bootstrap failures dispose allocated resources and restore previously installed defaults.

Each builder is single-use. `WithGlobalInstallation(false)` builds isolated resources for explicit application injection without changing `RxApp`, global locators, or converter/provider registries. The resulting application still exposes its configured scheduler values. Calling a scheduler method with its second argument set to `false` records the scheduler on the application without replacing that global scheduler. Configure defaults before constructing commands or other scheduler-dependent objects.

Application disposal restores previous global defaults when the disposed application still owns them. Nested applications also restore correctly when disposed out of creation order. A caller-provided `MessageBus` or `SuspensionHost` stays caller-owned: bootstrap subscriptions are removed, but the external object is not disposed. Registered service instances are likewise caller-owned; use `app.AddDisposable(instance)` when application teardown should dispose one.

| Modular facade | Existing configuration it accesses |
| --- | --- |
| `RxSchedulers.MainThreadScheduler` | `RxApp.MainThreadScheduler` |
| `RxSchedulers.TaskpoolScheduler` | `RxApp.TaskpoolScheduler` |
| `RxState.DefaultExceptionHandler` | `RxApp.DefaultExceptionHandler` |
| `RxState.ServiceLocator` | `Locator.Current` |
| `RxState.MessageBus` | `MessageBus.Current` |
| `RxSuspension.SuspensionHost` | The currently installed suspension host |

## Observing external object models

Reactive objects already supply `Changed` and `Changing`. A registered `IObservableForProperty` adapts an external model that exposes a different event mechanism. Existing reactive notifications take precedence. For other models, the provider with the highest positive affinity is selected, with the latest registration breaking ties.

```ts
class ExternalCounter {
  value = 0;
  notifications = new Subject<void>();
  increment() { this.value++; this.notifications.next(); }
}

const app = RxAppBuilder.Create()
  .WithObservableForProperty({
    GetAffinityForObject(type, propertyName, beforeChanged) {
      return type === ExternalCounter && propertyName === 'value' && !beforeChanged
        ? 10 : 0;
    },
    GetNotificationForProperty(sender) {
      return sender.notifications;
    },
  })
  .Build();

const counter = new ExternalCounter();
const subscription = WhenAnyValue(counter, 'value').subscribe(console.log);
counter.increment(); // Initial 0, then 1.
subscription.unsubscribe();
app.Dispose();
```

Provider streams emit notifications after mutations, or before mutations when `beforeChanged` is requested. They must not emit an initial value: `WhenAnyValue` and `ObservableForProperty` read and emit the initial property value themselves. Support both notification directions if before-change observation is needed. Provider subscriptions participate in nested-path rewiring and unsubscribe when the observation is disposed. New provider registrations affect new subscriptions and later path rebuilds; they do not proactively replace providers on already-subscribed paths.

## Binding conversion

`WithConverter`, `WithFallbackConverter`, and `WithSetMethodConverter` register converters in the corresponding registries of the installed `ConverterService`. `WithConverters(service => ...)` exposes all three registries for custom configuration. Default converters are included in each application. Custom typed converters can use a higher affinity to replace a default conversion without modifying the default registry.

```ts
const app = RxAppBuilder.Create()
  .WithConverter(new BindingTypeConverter(
    String, Number,
    text => Conversion.Success(Number(text) * 100),
    100,
  ))
  .Build();
```

This setup API does not perform CLR assembly scanning or load native UI platform modules. Register JavaScript view factories and integrations explicitly. Application setup configures persistence; initiate startup with `IsResuming.next()` or `IsLaunchingNew.next()`, and attach browser lifecycle handling separately when needed.
