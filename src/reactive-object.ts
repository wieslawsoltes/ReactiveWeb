import { EMPTY, Observable, Subject, Subscription, combineLatest, distinctUntilChanged, isObservable, map, merge, switchMap } from 'rxjs';
import { Disposable, type IDisposable } from './disposables.js';
import { RxApp } from './rx-app.js';
import { getPropertyChangeObservable } from './providers.js';

export interface IReactivePropertyChangedEventArgs<TSender = object, TValue = unknown> {
  readonly Sender: TSender;
  readonly PropertyName: string;
  readonly Value: TValue;
  readonly OldValue: TValue | undefined;
  readonly sender: TSender;
  readonly propertyName: string;
  readonly value: TValue;
  readonly oldValue: TValue | undefined;
}
export type PropertyChangedEvent<TSender = object, TValue = unknown> = IReactivePropertyChangedEventArgs<TSender, TValue>;
export type PropertyPath<T = any> = string | readonly (string | number)[] | ((source: T) => unknown);
export interface ObservePropertyOptions<T = unknown> {
  beforeChange?: boolean;
  skipInitial?: boolean;
  distinct?: boolean;
  comparer?: (previous: T, current: T) => boolean;
}

function event<TSender, TValue>(sender: TSender, propertyName: string, value: TValue, oldValue?: TValue): PropertyChangedEvent<TSender, TValue> {
  return { Sender: sender, PropertyName: propertyName, Value: value, OldValue: oldValue,
    sender, propertyName, value, oldValue };
}

/** Stores reactive values separately from accessors, so property setters never recurse. */
export class ReactiveObject implements IDisposable {
  private readonly values = new Map<string, unknown>();
  private readonly changingSubject = new Subject<PropertyChangedEvent<any>>();
  private readonly changedSubject = new Subject<PropertyChangedEvent<any>>();
  private readonly exceptionSubject = new Subject<unknown>();
  private suppressCount = 0;
  private delayCount = 0;
  private pending = new Map<string, { oldValue: unknown; value: unknown }>();
  private disposed = false;
  readonly Changing: Observable<PropertyChangedEvent<this>> = this.changingSubject.asObservable();
  readonly Changed: Observable<PropertyChangedEvent<this>> = this.changedSubject.asObservable();
  readonly ThrownExceptions = this.exceptionSubject.asObservable();
  readonly PropertyChanging = this.Changing;
  readonly PropertyChanged = this.Changed;
  readonly changing = this.Changing;
  readonly changed = this.Changed;
  readonly thrownExceptions = this.ThrownExceptions;

  constructor(initialValues?: Record<string, unknown>) {
    if (initialValues) defineReactiveProperties(this, initialValues);
  }
  get IsDisposed(): boolean { return this.disposed; }
  get AreChangeNotificationsEnabled(): boolean { return !this.disposed && this.suppressCount === 0; }
  GetChangedObservable(): Observable<PropertyChangedEvent<this>> { return this.Changed; }
  GetChangingObservable(): Observable<PropertyChangedEvent<this>> { return this.Changing; }
  GetThrownExceptionsObservable(): Observable<unknown> { return this.ThrownExceptions; }
  GetValue<T>(propertyName: string, fallback?: T): T {
    return (this.values.has(propertyName) ? this.values.get(propertyName) : fallback) as T;
  }
  getValue<T>(propertyName: string, fallback?: T): T { return this.GetValue(propertyName, fallback); }
  SetValue<T>(propertyName: string, value: T): T { return this.RaiseAndSetIfChanged(propertyName, value); }
  setValue<T>(propertyName: string, value: T): T { return this.SetValue(propertyName, value); }
  RaiseAndSetIfChanged<T>(propertyName: string, value: T): T {
    const previous = this.values.get(propertyName) as T;
    if (Object.is(previous, value) && this.values.has(propertyName)) return value;
    this.RaisePropertyChanging(propertyName, previous, value);
    this.values.set(propertyName, value);
    this.RaisePropertyChanged(propertyName, previous, value);
    return value;
  }
  raiseAndSetIfChanged<T>(propertyName: string, value: T): T { return this.RaiseAndSetIfChanged(propertyName, value); }
  RaisePropertyChanging(propertyName: string, oldValue?: unknown, newValue?: unknown): void {
    if (!this.AreChangeNotificationsEnabled) return;
    if (arguments.length < 2) oldValue = this.readProperty(propertyName);
    if (arguments.length < 3) newValue = oldValue;
    if (this.delayCount > 0) {
      if (!this.pending.has(propertyName)) this.pending.set(propertyName, { oldValue, value: newValue });
      return;
    }
    this.changingSubject.next(event(this, propertyName, newValue, oldValue));
  }
  RaisePropertyChanged(propertyName: string, oldValue?: unknown, newValue?: unknown): void {
    if (!this.AreChangeNotificationsEnabled) return;
    if (arguments.length < 3) newValue = this.readProperty(propertyName);
    if (this.delayCount > 0) {
      const pending = this.pending.get(propertyName);
      this.pending.delete(propertyName);
      this.pending.set(propertyName, { oldValue: pending ? pending.oldValue : oldValue, value: newValue });
      return;
    }
    this.changedSubject.next(event(this, propertyName, newValue, oldValue));
  }
  private readProperty(propertyName: string): unknown {
    return propertyName in this ? (this as any)[propertyName] : this.values.get(propertyName);
  }
  SuppressChangeNotifications(): IDisposable & { unsubscribe(): void } {
    if (this.disposed) return Disposable.Empty;
    this.suppressCount++;
    return Disposable.Create(() => { this.suppressCount--; });
  }
  suppressChangeNotifications(): IDisposable & { unsubscribe(): void } { return this.SuppressChangeNotifications(); }
  DelayChangeNotifications(): IDisposable & { unsubscribe(): void } {
    if (this.disposed) return Disposable.Empty;
    this.delayCount++;
    return Disposable.Create(() => {
      this.delayCount--;
      if (this.delayCount || !this.pending.size) return;
      const pending = this.pending;
      this.pending = new Map();
      if (!this.AreChangeNotificationsEnabled) return;
      for (const [propertyName, change] of pending) {
        this.RaisePropertyChanging(propertyName, change.oldValue, change.value);
        this.RaisePropertyChanged(propertyName, change.oldValue, change.value);
      }
    });
  }
  delayChangeNotifications(): IDisposable & { unsubscribe(): void } { return this.DelayChangeNotifications(); }
  ReportException(error: unknown): void {
    if (this.exceptionSubject.observed) this.exceptionSubject.next(error);
    else RxApp.HandleException(error);
  }
  ObservableForProperty<T = unknown>(path: PropertyPath<this>, options?: ObservePropertyOptions<T>): Observable<PropertyChangedEvent<this, T>> {
    return ObservableForProperty<this, T>(this, path, options);
  }
  WhenAnyValue<TKey extends keyof this>(path: TKey): Observable<this[TKey]>;
  WhenAnyValue<T>(path: (source: this) => T): Observable<T>;
  WhenAnyValue<T1, T2>(first: (source: this) => T1, second: (source: this) => T2): Observable<[T1, T2]>;
  WhenAnyValue<T1, TResult>(path: (source: this) => T1, selector: (value: T1) => TResult): Observable<TResult>;
  WhenAnyValue<T1, T2, TResult>(first: (source: this) => T1, second: (source: this) => T2, selector: (first: T1, second: T2) => TResult): Observable<TResult>;
  WhenAnyValue<T = unknown>(...pathsAndSelector: any[]): Observable<T>;
  WhenAnyValue<T = unknown>(...pathsAndSelector: any[]): Observable<T> { return WhenAnyValue<this, T>(this, ...pathsAndSelector); }
  whenAnyValue<T = unknown>(...pathsAndSelector: any[]): Observable<T> { return this.WhenAnyValue(...pathsAndSelector); }
  WhenAny<T = unknown>(...pathsAndSelector: any[]): Observable<T> { return WhenAny<this, T>(this, ...pathsAndSelector); }
  WhenAnyDynamic<T = unknown>(...pathsAndSelector: any[]): Observable<T> { return this.WhenAny(...pathsAndSelector); }
  WhenAnyObservable<T = unknown>(...pathsAndSelector: any[]): Observable<T> { return WhenAnyObservable<this, T>(this, ...pathsAndSelector); }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.pending.clear();
    this.changingSubject.complete();
    this.changedSubject.complete();
    this.exceptionSubject.complete();
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}

const definedProperties = new WeakMap<ReactiveObject, Set<string>>();
export function DefineReactiveProperty<T>(target: ReactiveObject, propertyName: string, initialValue: T): void {
  const defined = definedProperties.get(target) ?? new Set<string>();
  if (!defined.has(propertyName) && propertyName in target) throw new TypeError(`Property '${propertyName}' conflicts with an existing member.`);
  const own = Object.getOwnPropertyDescriptor(target, propertyName);
  if (own && !own.configurable) throw new TypeError(`Property '${propertyName}' is not configurable.`);
  Object.defineProperty(target, propertyName, {
    enumerable: true, configurable: true,
    get() { return this.GetValue(propertyName); },
    set(value: T) { this.RaiseAndSetIfChanged(propertyName, value); }
  });
  defined.add(propertyName);
  definedProperties.set(target, defined);
  const suppressed = target.SuppressChangeNotifications();
  try { target.SetValue(propertyName, initialValue); } finally { suppressed.Dispose(); }
}
export const defineReactiveProperty = DefineReactiveProperty;
export function defineReactiveProperties<T extends ReactiveObject, TProperties extends Record<string, unknown>>(target: T, properties: TProperties): T & TProperties {
  for (const [key, value] of Object.entries(properties)) DefineReactiveProperty(target, key, value);
  return target as T & TProperties;
}

/** Converts a member-access selector or dotted/bracketed path into a safe property chain. */
export function getPropertyPath<T>(path: PropertyPath<T>): string[] {
  if (Array.isArray(path)) {
    if (!path.length) throw new TypeError('A property path must contain at least one property.');
    return path.map(String);
  }
  if (typeof path === 'string') {
    const result: string[] = [];
    const expression = /(?:^|\.)([^.\[\]]+)|\[(?:(\d+)|"((?:[^"\\]|\\.)*)"|'((?:[^'\\]|\\.)*)')\]/g;
    let match: RegExpExecArray | null;
    let end = 0;
    while ((match = expression.exec(path))) {
      if (match.index !== end) throw new TypeError(`Invalid property path: ${path}`);
      result.push(match[1] ?? match[2] ?? (match[3] !== undefined ? JSON.parse(`"${match[3]}"`) : match[4].replace(/\\'/g, "'")));
      end = expression.lastIndex;
    }
    if (!result.length || end !== path.length) throw new TypeError(`Invalid property path: ${path}`);
    return result;
  }
  if (typeof path !== 'function') throw new TypeError('Expected a property path or member-access selector.');
  const chains = new WeakMap<object, string[]>();
  const create = (chain: string[]): object => {
    const proxy = new Proxy({}, {
      get(_target, property) {
        if (typeof property === 'symbol') throw new TypeError('Property selectors must use string keys.');
        return create([...chain, property]);
      }
    });
    chains.set(proxy, chain);
    return proxy;
  };
  const selected = path(create([]) as T);
  const chain = selected && typeof selected === 'object' ? chains.get(selected) : undefined;
  if (!chain?.length) throw new TypeError('Selectors must return a single property access, such as x => x.Person.Name.');
  return chain;
}

function readPath(source: unknown, parts: readonly string[]): unknown {
  let value: any = source;
  for (const part of parts) {
    if (value == null) return undefined;
    value = part in Object(value) ? value[part] : typeof value.GetValue === 'function' ? value.GetValue(part) : undefined;
  }
  return value;
}

/** Observes every reactive object in a chain and detaches old branches on replacement. */
export function ObservableForProperty<TSource extends object, TValue = unknown>(source: TSource, path: PropertyPath<TSource>, options: ObservePropertyOptions<TValue> = {}): Observable<PropertyChangedEvent<TSource, TValue>> {
  const parts = getPropertyPath(path);
  const propertyName = parts.join('.');
  const observable = new Observable<PropertyChangedEvent<TSource, TValue>>(subscriber => {
    let subscriptions = new Subscription();
    let first = true;
    let previous: TValue | undefined;
    let rebuilding = false;
    let rebuildAgain = false;
    const emit = () => {
      const value = readPath(source, parts) as TValue;
      const oldValue = previous;
      previous = value;
      if (first) {
        first = false;
        if (options.skipInitial) return;
      }
      subscriber.next(event(source, propertyName, value, oldValue));
    };
    const rebuild = () => {
      if (subscriber.closed) return;
      if (rebuilding) { rebuildAgain = true; return; }
      rebuilding = true;
      try {
        do {
          rebuildAgain = false;
          subscriptions.unsubscribe();
          subscriptions = new Subscription();
          let current: any = source;
          for (const part of parts) {
            if (current == null) break;
            const candidateChanges = current.Changed ?? current.changed;
            const changes = isObservable(candidateChanges) ? candidateChanges : getPropertyChangeObservable(current, part, false);
            if (isObservable(changes)) {
              subscriptions.add(changes.subscribe({
                next: (change: any) => {
                  if (change?.PropertyName && change.PropertyName !== part || change?.propertyName && change.propertyName !== part) return;
                  rebuild();
                  if (!options.beforeChange) emit();
                },
                error: error => subscriber.error(error)
              }));
            }
            if (options.beforeChange) {
              const candidateChanging = current.Changing ?? current.changing;
              const changing = isObservable(candidateChanging) ? candidateChanging : getPropertyChangeObservable(current, part, true);
              if (isObservable(changing)) subscriptions.add(changing.subscribe({
                next: (change: any) => {
                  if (change?.PropertyName && change.PropertyName !== part || change?.propertyName && change.propertyName !== part) return;
                  emit();
                },
                error: error => subscriber.error(error)
              }));
            }
            current = part in Object(current) ? current[part] : typeof current.GetValue === 'function' ? current.GetValue(part) : undefined;
          }
        } while (rebuildAgain && !subscriber.closed);
      } catch (error) { subscriber.error(error); }
      finally { rebuilding = false; }
    };
    rebuild();
    if (!subscriber.closed) {
      try { emit(); } catch (error) { subscriber.error(error); }
    }
    return () => subscriptions.unsubscribe();
  });
  if (options.distinct === false) return observable;
  const comparer = options.comparer ?? Object.is;
  return observable.pipe(distinctUntilChanged((previous, current) => comparer(previous.Value, current.Value)));
}
export const observableForProperty = ObservableForProperty;

function splitObservationArguments(args: any[]): { paths: PropertyPath[]; selector?: (...values: any[]) => unknown } {
  if (!args.length) throw new TypeError('At least one property path is required.');
  const paths = [...args];
  let selector: ((...values: any[]) => unknown) | undefined;
  if (paths.length > 1 && typeof paths[paths.length - 1] === 'function') {
    const candidate = paths[paths.length - 1];
    let isPath = false;
    if (candidate.length <= 1 && !paths.slice(0, -1).every(path => typeof path === 'string' || Array.isArray(path))) {
      try { getPropertyPath(candidate); isPath = true; } catch { /* A result selector is not a member-access path. */ }
    }
    if (!isPath) selector = paths.pop();
  }
  return { paths, selector };
}

export function WhenAnyValue<TSource extends object, TKey extends keyof TSource>(source: TSource, path: TKey): Observable<TSource[TKey]>;
export function WhenAnyValue<TSource extends object, TValue>(source: TSource, path: (source: TSource) => TValue): Observable<TValue>;
export function WhenAnyValue<TSource extends object, T1, T2>(source: TSource, first: (source: TSource) => T1, second: (source: TSource) => T2): Observable<[T1, T2]>;
export function WhenAnyValue<TSource extends object, T1, TResult>(source: TSource, path: (source: TSource) => T1, selector: (value: T1) => TResult): Observable<TResult>;
export function WhenAnyValue<TSource extends object, T1, T2, TResult>(source: TSource, first: (source: TSource) => T1, second: (source: TSource) => T2, selector: (first: T1, second: T2) => TResult): Observable<TResult>;
export function WhenAnyValue<TSource extends object, TResult = unknown>(source: TSource, ...pathsAndSelector: any[]): Observable<TResult>;
export function WhenAnyValue<TSource extends object, TResult = unknown>(source: TSource, ...pathsAndSelector: any[]): Observable<TResult> {
  const { paths, selector } = splitObservationArguments(pathsAndSelector);
  const observations = paths.map(path => ObservableForProperty(source, path).pipe(map(change => change.Value)));
  if (observations.length === 1 && !selector) return observations[0] as Observable<TResult>;
  return combineLatest(observations).pipe(map(values => (selector ? selector(...values) : values) as TResult));
}
export const whenAnyValue = WhenAnyValue;
export function WhenAny<TSource extends object, TResult = unknown>(source: TSource, ...pathsAndSelector: any[]): Observable<TResult> {
  const args = [...pathsAndSelector];
  const explicitSelector = args.length > 1 && typeof args[args.length - 1] === 'function' ? args.pop() as (...values: any[]) => unknown : undefined;
  const { paths, selector = explicitSelector } = splitObservationArguments(args);
  const observations = paths.map(path => ObservableForProperty(source, path));
  if (observations.length === 1 && !selector) return observations[0] as Observable<TResult>;
  return combineLatest(observations).pipe(map(values => (selector ? selector(...values) : values) as TResult));
}
export const whenAny = WhenAny;
export const WhenAnyDynamic = WhenAny;
export const whenAnyDynamic = WhenAny;
export function WhenAnyObservable<TSource extends object, TValue = unknown>(source: TSource, ...pathsAndSelector: any[]): Observable<TValue> {
  const { paths, selector } = splitObservationArguments(pathsAndSelector);
  const streams = paths.map(path => WhenAnyValue<TSource, Observable<unknown> | null | undefined>(source, path).pipe(
    switchMap(value => value == null ? EMPTY : value)
  ));
  return (selector ? combineLatest(streams).pipe(map(values => selector(...values))) : merge(...streams)) as Observable<TValue>;
}
export const whenAnyObservable = WhenAnyObservable;
