import test from 'node:test';
import assert from 'node:assert/strict';
import { Subject, queueScheduler, asapScheduler, asyncScheduler, firstValueFrom } from 'rxjs';
import { RxAppBuilder, DefaultRxAppBuilder, RxSchedulers, RxState, RxSuspension, ReactiveApplication } from '../dist/builder.js';
import { RxApp } from '../dist/rx-app.js';
import { Locator, ViewLocator, MessageBus, ServiceLocator } from '../dist/services.js';
import { InMemorySuspensionDriver, SuspensionHost } from '../dist/persistence.js';
import { ObservablePropertyProviders, ObservablePropertyProviderRegistry } from '../dist/providers.js';
import { WhenAnyValue, ObservableForProperty } from '../dist/reactive-object.js';
import { ConverterService, BindingTypeConverter, Conversion } from '../dist/converters.js';


test('builder installs concrete services, view registrations, schedulers and exception defaults', () => {
  class ViewModel { Name = 'test'; }
  const original = { services: Locator.Current, main: RxApp.MainThreadScheduler, task: RxApp.TaskpoolScheduler, views: ViewLocator.Current, error: RxApp.DefaultExceptionHandler };
  const errors: unknown[] = []; const token = Symbol('value'); let configured = false;
  const app = new DefaultRxAppBuilder().WithMainThreadScheduler(asapScheduler).WithTaskPoolScheduler(queueScheduler)
    .WithExceptionHandler(error => errors.push(error)).WithInstance(42, token)
    .RegisterSingletonViewModel(ViewModel).RegisterView(ViewModel, vm => ({ vm }))
    .WithRegistrationOnBuild(current => { assert.equal(Locator.Current, current.Services); configured = true; }).BuildApp();
  assert(configured); assert.equal(RxSchedulers.MainThreadScheduler, asapScheduler); assert.equal(RxSchedulers.TaskpoolScheduler, queueScheduler);
  assert.equal(Locator.Current.GetService(token), 42); assert.equal(app.Services.GetService(ReactiveApplication), app);
  assert.equal(app.Services.GetService(ServiceLocator), app.Services);
  const viewModel = app.Services.GetService(ViewModel)!; assert.equal(app.Services.GetService(ViewModel), viewModel);
  assert.equal(app.Views.ResolveView(viewModel).vm, viewModel);
  RxState.HandleException('handled'); assert.deepEqual(errors, ['handled']);
  app.Dispose(); assert.equal(Locator.Current, original.services); assert.equal(ViewLocator.Current, original.views);
  assert.equal(RxApp.MainThreadScheduler, original.main); assert.equal(RxApp.TaskpoolScheduler, original.task); assert.equal(RxApp.DefaultExceptionHandler, original.error);
});

test('builder is single-use and local installation leaves global resources untouched', () => {
  const original = Locator.Current, main = RxApp.MainThreadScheduler;
  const builder = RxAppBuilder.Create().WithGlobalInstallation(false).WithMainThreadScheduler(asyncScheduler);
  const app = builder.Build(); assert.equal(Locator.Current, original); assert.equal(RxApp.MainThreadScheduler, main);
  assert.equal(app.MainThreadScheduler, asyncScheduler); assert.throws(() => builder.Build(), /already/); assert.throws(() => builder.WithViews(() => {}), /already/);
  app.Dispose(); app.Dispose(); assert(app.IsDisposed);
});

test('nested builders restore global defaults even when disposed out of creation order', () => {
  const original = Locator.Current, originalMain = RxApp.MainThreadScheduler;
  const first = RxAppBuilder.Create().WithMainThreadScheduler(asapScheduler).Build();
  const second = RxAppBuilder.Create().WithMainThreadScheduler(asyncScheduler).Build();
  first.Dispose(); assert.equal(Locator.Current, second.Services); assert.equal(RxApp.MainThreadScheduler, asyncScheduler);
  second.Dispose(); assert.equal(Locator.Current, original); assert.equal(RxApp.MainThreadScheduler, originalMain);
});

test('failing bootstrap cleans resources and restores globals', () => {
  const original = Locator.Current; let cleaned = 0; let failedApp: ReactiveApplication | undefined;
  assert.throws(() => RxAppBuilder.Create().WithRegistrationOnBuild(app => { failedApp = app; app.AddDisposable(() => cleaned++); throw Error('bootstrap failed'); }).Build(), /bootstrap failed/);
  assert.equal(cleaned, 1); assert.equal(Locator.Current, original); assert(failedApp!.IsDisposed); assert.equal(failedApp!.Services.HasRegistration(ReactiveApplication), false);
});

test('builder configures real suspension load and save operations while preserving externally owned resources', async () => {
  const driver = new InMemorySuspensionDriver<{ count: number }>(); driver.SaveState({ count: 5 });
  const bus = new MessageBus(), host = new SuspensionHost(() => ({ count: 0 }));
  const app = RxAppBuilder.Create().WithMessageBus(bus).WithSuspensionHost(host).ConfigureSuspensionDriver(driver).Build();
  assert.equal(RxSuspension.SuspensionHost, host); host.IsResuming.next(); assert.deepEqual(host.AppState, { count: 5 });
  host.AppState = { count: 6 }; host.ShouldPersistState.next(); assert.deepEqual(driver.LoadState(), { count: 6 });
  app.Dispose(); const messages: number[] = []; bus.Listen<number>('number').subscribe(value => messages.push(value)); bus.SendMessage(1, 'number'); assert.deepEqual(messages, [1]);
  host.AppState = { count: 7 }; host.ShouldPersistState.next(); assert.deepEqual(driver.LoadState(), { count: 6 }); host.Dispose(); bus.Dispose();
});

test('view and service singleton versus transient configuration executes actual factories', () => {
  class Model {} const vm = new Model(); let created = 0;
  const app = RxAppBuilder.Create().WithGlobalInstallation(false).RegisterViewModel(Model)
    .WithService('singleton', () => ++created, { lifetime: 'singleton', contract: 'x' })
    .RegisterSingletonView(Model, model => ({ model })).Build();
  assert.notEqual(app.Services.GetService(Model), app.Services.GetService(Model));
  assert.equal(app.Services.GetService('singleton', 'x'), 1); assert.equal(app.Services.GetService('singleton', 'x'), 1);
  assert.equal(app.Views.ResolveView(vm), app.Views.ResolveView(vm)); app.Dispose();
});

test('custom property provider participates in WhenAnyValue and detaches on subscription disposal', () => {
  class ExternalModel { value = 1; changes = new Subject<void>(); before = new Subject<void>(); }
  const app = RxAppBuilder.Create().WithObservableForProperty({
    GetAffinityForObject: type => type === ExternalModel ? 10 : 0,
    GetNotificationForProperty: (sender, property, before) => before ? (sender as ExternalModel).before : (sender as ExternalModel).changes,
  }).Build();
  const model = new ExternalModel(), values: unknown[] = [], beforeValues: unknown[] = [];
  const subscription = WhenAnyValue(model, 'value').subscribe(value => values.push(value));
  const beforeSubscription = ObservableForProperty(model, 'value', { beforeChange: true }).subscribe(change => beforeValues.push(change.Value));
  model.before.next(); model.value = 2; model.changes.next(); model.before.next(); model.value = 3; model.changes.next();
  assert.deepEqual(values, [1, 2, 3]); assert.deepEqual(beforeValues, [1, 2]);
  subscription.unsubscribe(); beforeSubscription.unsubscribe(); assert.equal(model.changes.observed, false); assert.equal(model.before.observed, false); app.Dispose();
});

test('property provider registry selects highest affinity and reversible recent overrides', () => {
  const registry = new ObservablePropertyProviderRegistry(), source = new Subject<void>(), object = {};
  const first = { GetAffinityForObject: () => 10, GetNotificationForProperty: () => source };
  const second = { ...first }; registry.Register(first); const registration = registry.Register(second);
  assert.equal(registry.GetProvider(object, 'value'), second); registration.Dispose(); assert.equal(registry.GetProvider(object, 'value'), first);
  registry.Register({ ...first, GetAffinityForObject: () => 0 }); assert.equal(registry.GetProvider(object, 'value'), first);
  assert.equal(registry.Observe(object, 'value'), source); registry.Clear(); assert.equal(registry.Observe(object, 'value'), undefined);
});

test('builder converter registration installs and restores the actual converter service', () => {
  const original = ConverterService.Current;
  const converter = new BindingTypeConverter(String, Number, value => Conversion.Success(Number(value) * 10), 100);
  const app = RxAppBuilder.Create().WithConverter(converter).Build();
  assert.equal(ConverterService.Current.Convert('2', Number), 20); assert.equal(app.Services.GetService(ConverterService), app.Converters);
  app.Dispose(); assert.equal(ConverterService.Current, original);
});
