import { Observable, ReplaySubject, Subject, Subscription, observeOn, type SchedulerLike } from 'rxjs';
import { Disposable, type IDisposable } from './disposables.js';

export type ServiceToken<T = unknown> = string | symbol | (abstract new (...args: any[]) => T);
type Registration<T> = { factory: () => T; singleton: boolean; created: boolean; resolving: boolean; value?: T };

/** Explicit tokens replace erased CLR generic type information. Last registration wins. */
export class ServiceLocator {
  private readonly registrations = new Map<ServiceToken<any>, Map<string, Registration<any>[]>>();
  Register<T>(factory: () => T, serviceType: ServiceToken<T>, contract?: string): IDisposable {
    if (typeof factory !== 'function') throw new TypeError('A service factory is required');
    return this.registerRecord(serviceType, contract, { factory, singleton: false, created: false, resolving: false });
  }
  RegisterConstant<T>(value: T, serviceType: ServiceToken<T>, contract?: string): IDisposable {
    return this.registerRecord(serviceType, contract, { factory: () => value, singleton: true, created: true, resolving: false, value });
  }
  RegisterLazySingleton<T>(factory: () => T, serviceType: ServiceToken<T>, contract?: string): IDisposable {
    return this.registerRecord(serviceType, contract, { factory, singleton: true, created: false, resolving: false });
  }
  private registerRecord<T>(token: ServiceToken<T>, contract: string | undefined, registration: Registration<T>): IDisposable {
    if (token === undefined || token === null) throw new TypeError('An explicit service token is required');
    let contracts = this.registrations.get(token);
    if (!contracts) this.registrations.set(token, contracts = new Map());
    let records = contracts.get(contract ?? '');
    if (!records) contracts.set(contract ?? '', records = []);
    records.push(registration);
    return Disposable.Create(() => {
      const current = this.registrations.get(token)?.get(contract ?? '');
      const index = current?.indexOf(registration) ?? -1;
      if (index >= 0) current!.splice(index, 1);
      this.prune(token, contract);
    });
  }
  private prune(token: ServiceToken<any>, contract?: string): void {
    const contracts = this.registrations.get(token);
    if (contracts?.get(contract ?? '')?.length === 0) contracts.delete(contract ?? '');
    if (contracts?.size === 0) this.registrations.delete(token);
  }
  private resolveRegistration<T>(record: Registration<T>): T {
    if (record.singleton && record.created) return record.value as T;
    if (record.resolving) throw new Error('Circular service dependency detected');
    record.resolving = true;
    try {
      const value = record.factory();
      if (record.singleton) { record.value = value; record.created = true; }
      return value;
    } finally { record.resolving = false; }
  }
  GetService<T>(serviceType: ServiceToken<T>, contract?: string): T | undefined {
    const record = this.registrations.get(serviceType)?.get(contract ?? '')?.at(-1);
    return record ? this.resolveRegistration<T>(record) : undefined;
  }
  GetRequiredService<T>(serviceType: ServiceToken<T>, contract?: string): T {
    if (!this.HasRegistration(serviceType, contract)) throw new Error(`Service is not registered: ${String(serviceType)}${contract ? ` (${contract})` : ''}`);
    return this.GetService(serviceType, contract) as T;
  }
  GetServices<T>(serviceType: ServiceToken<T>, contract?: string): readonly T[] {
    return (this.registrations.get(serviceType)?.get(contract ?? '') ?? []).map(record => this.resolveRegistration<T>(record));
  }
  HasRegistration(serviceType: ServiceToken<any>, contract?: string): boolean {
    return (this.registrations.get(serviceType)?.get(contract ?? '')?.length ?? 0) > 0;
  }
  UnregisterCurrent(serviceType: ServiceToken<any>, contract?: string): void {
    this.registrations.get(serviceType)?.get(contract ?? '')?.pop(); this.prune(serviceType, contract);
  }
  UnregisterAll(serviceType: ServiceToken<any>, contract?: string): void {
    this.registrations.get(serviceType)?.delete(contract ?? ''); this.prune(serviceType, contract);
  }
  Clear(): void { this.registrations.clear(); }
  register<T>(token: ServiceToken<T>, factory: () => T, contract?: string): IDisposable { return this.Register(factory, token, contract); }
  resolve<T>(token: ServiceToken<T>, contract?: string): T | undefined { return this.GetService(token, contract); }
}

export class Locator {
  static Current = new ServiceLocator();
  static get CurrentMutable(): ServiceLocator { return this.Current; }
}

export type ViewFactory<TViewModel = any, TView = any> = (viewModel: TViewModel) => TView;
/** View factories support DOM nodes, IViewFor wrappers, and framework components. */
export class ViewLocator {
  static Current = new ViewLocator();
  private readonly views = new Map<Function, Map<string, ViewFactory[]>>();
  private readonly singletonViews = new WeakMap<object, number>();
  private readonly singletonRegistrations = new Set<IDisposable>();
  Register<TViewModel, TView>(type: abstract new (...args: any[]) => TViewModel, factory: ViewFactory<TViewModel, TView>, contract?: string): IDisposable {
    let contracts = this.views.get(type); if (!contracts) this.views.set(type, contracts = new Map());
    let factories = contracts.get(contract ?? ''); if (!factories) contracts.set(contract ?? '', factories = []);
    factories.push(factory);
    return Disposable.Create(() => {
      const index = factories!.indexOf(factory); if (index >= 0) factories!.splice(index, 1);
      if (!factories!.length) contracts!.delete(contract ?? '');
      if (!contracts!.size) this.views.delete(type);
    });
  }
  RegisterSingleton<TViewModel, TView>(type: abstract new (...args: any[]) => TViewModel, factory: ViewFactory<TViewModel, TView>, contract?: string): IDisposable {
    let created = false, view: TView;
    const factoryRegistration = this.Register(type, vm => {
      if (!created) {
        view = factory(vm); created = true;
        if (view !== null && (typeof view === 'object' || typeof view === 'function')) {
          const object = view as object; this.singletonViews.set(object, (this.singletonViews.get(object) ?? 0) + 1);
        }
      }
      return view!;
    }, contract);
    const registration = Disposable.Create(() => {
      this.singletonRegistrations.delete(registration); factoryRegistration.Dispose();
      if (created && view !== null && (typeof view === 'object' || typeof view === 'function')) {
        const object = view as object, remaining = (this.singletonViews.get(object) ?? 1) - 1;
        if (remaining > 0) this.singletonViews.set(object, remaining);
        else {
          this.singletonViews.delete(object);
          const disposable = view as { Dispose?: () => void; unsubscribe?: () => void; dispose?: () => void };
          if (typeof disposable.Dispose === 'function') disposable.Dispose();
          else if (typeof disposable.unsubscribe === 'function') disposable.unsubscribe();
          else if (typeof disposable.dispose === 'function') disposable.dispose();
        }
      }
    });
    this.singletonRegistrations.add(registration); return registration;
  }
  IsSingletonView(view: unknown): boolean {
    return view !== null && (typeof view === 'object' || typeof view === 'function') && (this.singletonViews.get(view as object) ?? 0) > 0;
  }
  ResolveView<TView = any>(viewModel: unknown, contract?: string): TView | undefined {
    if (viewModel === null || viewModel === undefined) return undefined;
    let prototype = Object.getPrototypeOf(viewModel);
    while (prototype) {
      const factory = this.views.get(prototype.constructor)?.get(contract ?? '')?.at(-1);
      if (factory) return factory(viewModel) as TView;
      prototype = Object.getPrototypeOf(prototype);
    }
    return undefined;
  }
  Clear(): void {
    const errors: unknown[] = [];
    for (const registration of [...this.singletonRegistrations]) { try { registration.Dispose(); } catch (error) { errors.push(error); } }
    this.views.clear(); if (errors.length) throw new AggregateError(errors, 'Singleton view disposal failed');
  }
}

type Channel<T> = { live: Subject<T>; latest: ReplaySubject<T> };
/** In-process typed message channels. A source failure does not poison the channel. */
export class MessageBus implements IDisposable {
  static Current = new MessageBus();
  private readonly channels = new Map<ServiceToken<any>, Map<string, Channel<any>>>();
  private readonly schedulers = new Map<ServiceToken<any>, Map<string, SchedulerLike>>();
  private readonly sources = new Subscription();
  private readonly errors = new Subject<unknown>();
  private disposed = false;
  get ThrownExceptions(): Observable<unknown> { return this.errors.asObservable(); }
  private channel<T>(token: ServiceToken<T>, contract?: string): Channel<T> {
    if (this.disposed) throw new Error('Message bus is disposed');
    if (token === undefined || token === null) throw new TypeError('An explicit message token is required');
    let contracts = this.channels.get(token); if (!contracts) this.channels.set(token, contracts = new Map());
    let channel = contracts.get(contract ?? '');
    if (!channel) contracts.set(contract ?? '', channel = { live: new Subject<T>(), latest: new ReplaySubject<T>(1) });
    return channel;
  }
  private schedule<T>(stream: Observable<T>, token: ServiceToken<T>, contract?: string): Observable<T> {
    const scheduler = this.schedulers.get(token)?.get(contract ?? ''); return scheduler ? stream.pipe(observeOn(scheduler)) : stream;
  }
  Listen<T>(messageType: ServiceToken<T>, contract?: string): Observable<T> {
    return this.schedule(this.channel(messageType, contract).live.asObservable(), messageType, contract);
  }
  ListenIncludeLatest<T>(messageType: ServiceToken<T>, contract?: string): Observable<T> {
    return this.schedule(this.channel(messageType, contract).latest.asObservable(), messageType, contract);
  }
  SendMessage<T>(message: T, messageType: ServiceToken<T>, contract?: string): void {
    const channel = this.channel(messageType, contract); channel.latest.next(message); channel.live.next(message);
  }
  RegisterMessageSource<T>(source: Observable<T>, messageType: ServiceToken<T>, contract?: string): IDisposable {
    this.channel(messageType, contract);
    const subscription = source.subscribe({ next: value => this.SendMessage(value, messageType, contract), error: error => this.errors.next(error) });
    this.sources.add(subscription);
    return Disposable.Create(() => { subscription.unsubscribe(); this.sources.remove(subscription); });
  }
  RegisterScheduler<T>(scheduler: SchedulerLike, messageType: ServiceToken<T>, contract?: string): void {
    let contracts = this.schedulers.get(messageType); if (!contracts) this.schedulers.set(messageType, contracts = new Map());
    contracts.set(contract ?? '', scheduler);
  }
  IsRegistered(messageType: ServiceToken<any>, contract?: string): boolean { return this.channels.get(messageType)?.has(contract ?? '') ?? false; }
  Dispose(): void {
    if (this.disposed) return; this.disposed = true; this.sources.unsubscribe();
    for (const contracts of this.channels.values()) for (const channel of contracts.values()) { channel.live.complete(); channel.latest.complete(); }
    this.channels.clear(); this.schedulers.clear(); this.errors.complete();
  }
  unsubscribe(): void { this.Dispose(); }
}
