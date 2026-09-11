import { type SchedulerLike } from 'rxjs';
import { CompositeDisposable, Disposable, type DisposableLike, type IDisposable } from './disposables.js';
import { RxApp, type RxAppOptions } from './rx-app.js';
import { Locator, MessageBus, ServiceLocator, ViewLocator, type ServiceToken, type ViewFactory } from './services.js';
import { SuspensionHost, type ISuspensionDriver } from './persistence.js';
import { ConverterService, type IBindingTypeConverter, type IBindingFallbackConverter, type ISetMethodBindingConverter } from './converters.js';
import { ObservablePropertyProviders, ObservablePropertyProviderRegistry, type IObservableForProperty } from './providers.js';

export class RxSchedulers {
  static get MainThreadScheduler(): SchedulerLike { return RxApp.MainThreadScheduler; }
  static set MainThreadScheduler(value: SchedulerLike) { RxApp.MainThreadScheduler = value; }
  static get TaskpoolScheduler(): SchedulerLike { return RxApp.TaskpoolScheduler; }
  static set TaskpoolScheduler(value: SchedulerLike) { RxApp.TaskpoolScheduler = value; }
}
export class RxState {
  static get DefaultExceptionHandler(): NonNullable<RxAppOptions['defaultExceptionHandler']> { return RxApp.DefaultExceptionHandler; }
  static set DefaultExceptionHandler(value: NonNullable<RxAppOptions['defaultExceptionHandler']>) { RxApp.DefaultExceptionHandler = value; }
  static get ServiceLocator(): ServiceLocator { return Locator.Current; }
  static get MessageBus(): MessageBus { return MessageBus.Current; }
  static HandleException(error: unknown): void { RxApp.HandleException(error); }
}
export class RxSuspension {
  static SuspensionHost = new SuspensionHost<any>();
}

type GlobalSnapshot = {
  services: ServiceLocator; views: ViewLocator; messages: MessageBus; suspension: SuspensionHost<any>;
  providers: ObservablePropertyProviderRegistry; converters: ConverterService; main: SchedulerLike; task: SchedulerLike;
  error: NonNullable<RxAppOptions['defaultExceptionHandler']>;
};
type GlobalFrame = { active: boolean; previous: GlobalSnapshot; installed: GlobalSnapshot };
const globalFrames: GlobalFrame[] = [];
function captureGlobals(): GlobalSnapshot {
  return { services: Locator.Current, views: ViewLocator.Current, messages: MessageBus.Current, suspension: RxSuspension.SuspensionHost, providers: ObservablePropertyProviders.Current, converters: ConverterService.Current, main: RxApp.MainThreadScheduler, task: RxApp.TaskpoolScheduler, error: RxApp.DefaultExceptionHandler };
}
function releaseGlobals(frame: GlobalFrame): void {
  frame.active = false;
  while (globalFrames.length && !globalFrames.at(-1)!.active) {
    const { previous, installed } = globalFrames.pop()!;
    if (Locator.Current === installed.services) Locator.Current = previous.services;
    if (ViewLocator.Current === installed.views) ViewLocator.Current = previous.views;
    if (MessageBus.Current === installed.messages) MessageBus.Current = previous.messages;
    if (RxSuspension.SuspensionHost === installed.suspension) RxSuspension.SuspensionHost = previous.suspension;
    if (ObservablePropertyProviders.Current === installed.providers) ObservablePropertyProviders.Current = previous.providers;
    if (ConverterService.Current === installed.converters) ConverterService.Current = previous.converters;
    if (RxApp.MainThreadScheduler === installed.main) RxApp.MainThreadScheduler = previous.main;
    if (RxApp.TaskpoolScheduler === installed.task) RxApp.TaskpoolScheduler = previous.task;
    if (RxApp.DefaultExceptionHandler === installed.error) RxApp.DefaultExceptionHandler = previous.error;
  }
}

export interface ServiceRegistrationOptions { contract?: string; lifetime?: 'transient' | 'singleton'; }
type AppConfiguration = (app: ReactiveApplication) => void | DisposableLike;
interface AppResources {
  services: ServiceLocator;
  views: ViewLocator;
  messages: MessageBus;
  suspension: SuspensionHost<any>;
  providers: ObservablePropertyProviderRegistry;
  converters: ConverterService;
  mainThread: SchedulerLike;
  taskpool: SchedulerLike;
}
/** A built application's registrations and bootstrap subscriptions share a disposable lifetime. */
export class ReactiveApplication implements IDisposable {
  readonly Services: ServiceLocator;
  readonly Views: ViewLocator;
  readonly MessageBus: MessageBus;
  readonly SuspensionHost: SuspensionHost<any>;
  readonly PropertyProviders: ObservablePropertyProviderRegistry;
  readonly Converters: ConverterService;
  readonly MainThreadScheduler: SchedulerLike;
  readonly TaskpoolScheduler: SchedulerLike;
  private readonly lifetime = new CompositeDisposable();
  constructor(resources: AppResources) {
    this.Services = resources.services; this.Views = resources.views; this.MessageBus = resources.messages;
    this.SuspensionHost = resources.suspension; this.PropertyProviders = resources.providers; this.Converters = resources.converters;
    this.MainThreadScheduler = resources.mainThread; this.TaskpoolScheduler = resources.taskpool;
  }
  get IsDisposed(): boolean { return this.lifetime.IsDisposed; }
  AddDisposable<T extends DisposableLike>(resource: T): T { return this.lifetime.Add(resource); }
  Dispose(): void { this.lifetime.Dispose(); }
  unsubscribe(): void { this.Dispose(); }
}

/** Explicit JS constructors/tokens replace CLR assembly scanning during fluent app setup. */
export class RxAppBuilder {
  private readonly configure: AppConfiguration[] = [];
  private readonly onBuild: AppConfiguration[] = [];
  private readonly schedulerOptions: RxAppOptions = {};
  private mainThread = RxApp.MainThreadScheduler;
  private taskpool = RxApp.TaskpoolScheduler;
  private messageBusFactory: () => MessageBus = () => new MessageBus();
  private suspensionFactory: () => SuspensionHost<any> = () => new SuspensionHost();
  private ownsMessageBus = true;
  private ownsSuspension = true;
  private installGlobals = true;
  private built = false;
  static Create(): RxAppBuilder { return new RxAppBuilder(); }
  private editable(): void { if (this.built) throw new Error('This application builder has already been built'); }
  WithMainThreadScheduler(scheduler: SchedulerLike, setRxApp = true): this {
    this.editable(); this.mainThread = scheduler; if (setRxApp) this.schedulerOptions.mainThreadScheduler = scheduler; return this;
  }
  WithTaskPoolScheduler(scheduler: SchedulerLike, setRxApp = true): this {
    this.editable(); this.taskpool = scheduler; if (setRxApp) this.schedulerOptions.taskpoolScheduler = scheduler; return this;
  }
  WithTaskpoolScheduler(scheduler: SchedulerLike, setRxApp = true): this { return this.WithTaskPoolScheduler(scheduler, setRxApp); }
  WithExceptionHandler(handler: NonNullable<RxAppOptions['defaultExceptionHandler']>): this {
    this.editable(); this.schedulerOptions.defaultExceptionHandler = handler; return this;
  }
  WithDefaultExceptionHandler(handler: NonNullable<RxAppOptions['defaultExceptionHandler']>): this { return this.WithExceptionHandler(handler); }
  /** Local application resources can be built without replacing the global facades. */
  WithGlobalInstallation(enabled: boolean): this { this.editable(); this.installGlobals = enabled; return this; }
  WithService<T>(token: ServiceToken<T>, factory: () => T, options: ServiceRegistrationOptions | string = {}): this {
    this.editable(); const settings = typeof options === 'string' ? { contract: options } : options;
    this.configure.push(app => settings.lifetime === 'singleton'
      ? app.Services.RegisterLazySingleton(factory, token, settings.contract)
      : app.Services.Register(factory, token, settings.contract));
    return this;
  }
  WithInstance<T>(instance: T, token: ServiceToken<T>, contract?: string): this {
    this.editable(); this.configure.push(app => app.Services.RegisterConstant(instance, token, contract)); return this;
  }
  WithServices(configure: (services: ServiceLocator) => void | DisposableLike): this {
    this.editable(); this.configure.push(app => configure(app.Services)); return this;
  }
  WithRegistration(configure: (services: ServiceLocator) => void | DisposableLike): this { return this.WithServices(configure); }
  WithRegistrationOnBuild(configure: AppConfiguration): this { this.editable(); this.onBuild.push(configure); return this; }
  WithViews(configure: (views: ViewLocator) => void | DisposableLike): this {
    this.editable(); this.configure.push(app => configure(app.Views)); return this;
  }
  ConfigureViewLocator(configure: (views: ViewLocator) => void | DisposableLike): this { return this.WithViews(configure); }
  RegisterView<TViewModel, TView>(viewModelType: abstract new (...args: any[]) => TViewModel, factory: ViewFactory<TViewModel, TView>, contract?: string): this {
    return this.WithViews(views => views.Register(viewModelType, factory, contract));
  }
  WithView<TViewModel, TView>(viewModelType: abstract new (...args: any[]) => TViewModel, factory: ViewFactory<TViewModel, TView>, contract?: string): this {
    return this.RegisterView(viewModelType, factory, contract);
  }
  RegisterSingletonView<TViewModel, TView>(viewModelType: abstract new (...args: any[]) => TViewModel, factory: ViewFactory<TViewModel, TView>, contract?: string): this {
    return this.WithViews(views => views.RegisterSingleton(viewModelType, factory, contract));
  }
  RegisterViewModel<T>(type: new (...args: any[]) => T, factory: () => T = () => new type(), contract?: string): this {
    return this.WithService(type, factory, { contract });
  }
  RegisterSingletonViewModel<T>(type: new (...args: any[]) => T, factory: () => T = () => new type(), contract?: string): this {
    return this.WithService(type, factory, { contract, lifetime: 'singleton' });
  }
  RegisterConstantViewModel<T extends object>(instance: T, token: ServiceToken<T> = instance.constructor as ServiceToken<T>, contract?: string): this {
    return this.WithInstance(instance, token, contract);
  }
  WithMessageBus(busOrConfigure?: MessageBus | ((bus: MessageBus) => void | DisposableLike)): this {
    this.editable();
    if (busOrConfigure instanceof MessageBus) { this.messageBusFactory = () => busOrConfigure; this.ownsMessageBus = false; }
    else {
      this.messageBusFactory = () => new MessageBus(); this.ownsMessageBus = true;
      if (busOrConfigure) this.configure.push(app => busOrConfigure(app.MessageBus));
    }
    return this;
  }
  WithSuspensionHost<T>(hostOrFactory: SuspensionHost<T> | (() => SuspensionHost<T>)): this {
    this.editable();
    if (typeof hostOrFactory === 'function') { this.suspensionFactory = hostOrFactory; this.ownsSuspension = true; }
    else { this.suspensionFactory = () => hostOrFactory; this.ownsSuspension = false; }
    return this;
  }
  WithSuspension<T>(createNewAppState: () => T, driver: ISuspensionDriver<T>): this {
    this.WithSuspensionHost(() => new SuspensionHost(createNewAppState)); return this.ConfigureSuspensionDriver(driver);
  }
  ConfigureSuspensionDriver<T>(driverOrFactory: ISuspensionDriver<T> | ((app: ReactiveApplication) => ISuspensionDriver<T>)): this {
    this.editable(); this.configure.push(app => app.SuspensionHost.SetupDefaultSuspendResume(typeof driverOrFactory === 'function' ? driverOrFactory(app) : driverOrFactory)); return this;
  }
  WithObservableForProperty(provider: IObservableForProperty): this {
    this.editable(); this.configure.push(app => app.PropertyProviders.Register(provider)); return this;
  }
  WithConverters(configure: (converters: ConverterService) => void | DisposableLike): this {
    this.editable(); this.configure.push(app => configure(app.Converters)); return this;
  }
  WithConverter(converter: IBindingTypeConverter<any, any>): this { return this.WithConverters(service => service.TypedConverters.Register(converter)); }
  WithFallbackConverter(converter: IBindingFallbackConverter): this { return this.WithConverters(service => service.FallbackConverters.Register(converter)); }
  WithSetMethodConverter(converter: ISetMethodBindingConverter): this { return this.WithConverters(service => service.SetMethodConverters.Register(converter)); }
  Build(): ReactiveApplication {
    this.editable(); this.built = true;
    const messages = this.messageBusFactory();
    let suspension: SuspensionHost<any>;
    try { suspension = this.suspensionFactory(); }
    catch (error) { if (this.ownsMessageBus) messages.Dispose(); throw error; }
    const app = new ReactiveApplication({ services: new ServiceLocator(), views: new ViewLocator(), messages, suspension, providers: new ObservablePropertyProviderRegistry(), converters: new ConverterService(), mainThread: this.mainThread, taskpool: this.taskpool });
    // Add owned object cleanup first; bootstrap registrations are individually removable too.
    app.AddDisposable(() => app.Services.Clear()); app.AddDisposable(() => app.Views.Clear()); app.AddDisposable(() => app.PropertyProviders.Clear());
    app.AddDisposable(() => { app.Converters.TypedConverters.Clear(); app.Converters.FallbackConverters.Clear(); app.Converters.SetMethodConverters.Clear(); });
    if (this.ownsMessageBus) app.AddDisposable(app.MessageBus);
    if (this.ownsSuspension) app.AddDisposable(app.SuspensionHost);
    try {
      app.Services.RegisterConstant(app, ReactiveApplication);
      app.Services.RegisterConstant(app.Services, ServiceLocator);
      app.Services.RegisterConstant(app.Views, ViewLocator);
      app.Services.RegisterConstant(app.MessageBus, MessageBus);
      app.Services.RegisterConstant(app.SuspensionHost, SuspensionHost);
      app.Services.RegisterConstant(app.PropertyProviders, ObservablePropertyProviderRegistry);
      app.Services.RegisterConstant(app.Converters, ConverterService);
      for (const configure of this.configure) { const resource = configure(app); if (resource) app.AddDisposable(resource); }
      if (this.installGlobals) this.install(app);
      for (const configure of this.onBuild) { const resource = configure(app); if (resource) app.AddDisposable(resource); }
      return app;
    } catch (error) {
      try { app.Dispose(); } catch (cleanupError) { throw new AggregateError([error, cleanupError], 'Application setup and cleanup failed'); }
      throw error;
    }
  }
  BuildApp(): ReactiveApplication { return this.Build(); }
  private install(app: ReactiveApplication): void {
    const previous = captureGlobals();
    Locator.Current = app.Services; ViewLocator.Current = app.Views; MessageBus.Current = app.MessageBus;
    RxSuspension.SuspensionHost = app.SuspensionHost; ObservablePropertyProviders.Current = app.PropertyProviders; ConverterService.Current = app.Converters;
    RxApp.Configure(this.schedulerOptions);
    const frame: GlobalFrame = { active: true, previous, installed: captureGlobals() }; globalFrames.push(frame);
    app.AddDisposable(Disposable.Create(() => releaseGlobals(frame)));
  }
}
export class DefaultRxAppBuilder extends RxAppBuilder {}
export { RxAppBuilder as ReactiveWebBuilder, RxAppBuilder as ReactiveUIBuilder };
