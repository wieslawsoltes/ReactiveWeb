import { Observable, Subject, distinctUntilChanged, observeOn, type SchedulerLike } from 'rxjs';
import { SingleAssignmentDisposable, type IDisposable } from './disposables.js';
import { RxApp } from './rx-app.js';

export interface ObservableAsPropertyOptions<T> {
  initialValue?: T;
  scheduler?: SchedulerLike;
  deferSubscription?: boolean;
  comparer?: (previous: T, current: T) => boolean;
  onChanging?: (value: T, previous: T) => void;
  onChanged?: (value: T, previous: T) => void;
  /** ToProperty installs a read-only accessor unless the owner already declares a read-only getter. */
  installProperty?: boolean;
}
export interface PropertyNotificationTarget {
  RaisePropertyChanging(propertyName: string, oldValue?: unknown, newValue?: unknown): void;
  RaisePropertyChanged(propertyName: string, oldValue?: unknown, newValue?: unknown): void;
  ReportException?(error: unknown): void;
}

/** A cached observable value with .NET notification order and deterministic ownership. */
export class ObservableAsPropertyHelper<T> implements IDisposable {
  private value: T;
  private disposed = false;
  private subscribed = false;
  private readonly subscription = new SingleAssignmentDisposable();
  private readonly exceptionSubject = new Subject<unknown>();
  private readonly options: ObservableAsPropertyOptions<T>;
  readonly ThrownExceptions = this.exceptionSubject.asObservable();
  readonly thrownExceptions = this.ThrownExceptions;

  constructor(source: Observable<T>, options?: ObservableAsPropertyOptions<T>);
  constructor(source: Observable<T>, onChanged: (value: T, previous: T) => void, initialValue?: T, deferSubscription?: boolean, scheduler?: SchedulerLike);
  constructor(private readonly source: Observable<T>, optionsOrChanged: ObservableAsPropertyOptions<T> | ((value: T, previous: T) => void) = {}, initialValue?: T, deferSubscription = false, scheduler?: SchedulerLike) {
    this.options = typeof optionsOrChanged === 'function'
      ? { onChanged: optionsOrChanged, initialValue, deferSubscription, scheduler }
      : optionsOrChanged;
    this.value = this.options.initialValue as T;
    if (!this.options.deferSubscription) this.Subscribe();
  }
  get Value(): T { this.Subscribe(); return this.value; }
  get valueOrDefault(): T { return this.value; }
  get IsSubscribed(): boolean { return this.subscribed; }
  get IsDisposed(): boolean { return this.disposed; }
  get closed(): boolean { return this.disposed; }
  Subscribe(): void {
    if (this.disposed || this.subscribed) return;
    this.subscribed = true;
    const comparer = this.options.comparer ?? Object.is;
    const source = this.source.pipe(distinctUntilChanged(comparer), observeOn(this.options.scheduler ?? RxApp.MainThreadScheduler));
    this.subscription.Disposable = source.subscribe({
      next: value => {
        if (this.disposed || comparer(this.value, value)) return;
        const previous = this.value;
        try {
          this.options.onChanging?.(value, previous);
          if (this.disposed) return;
          this.value = value;
          this.options.onChanged?.(value, previous);
        } catch (error) { this.reportException(error); }
      },
      error: error => this.reportException(error)
    });
  }
  private reportException(error: unknown): void {
    if (this.disposed) return;
    if (this.exceptionSubject.observed) this.exceptionSubject.next(error);
    else RxApp.HandleException(error);
  }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    try { this.subscription.Dispose(); } finally { this.exceptionSubject.complete(); }
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}

function isOptions<T>(value: unknown): value is ObservableAsPropertyOptions<T> {
  return value !== null && typeof value === 'object' &&
    (Object.keys(value).length === 0 || ['initialValue', 'scheduler', 'deferSubscription', 'comparer', 'onChanging', 'onChanged', 'installProperty'].some(key => key in value));
}

export function ToProperty<T>(source: Observable<T>, owner: PropertyNotificationTarget, propertyName: string, options?: ObservableAsPropertyOptions<T>): ObservableAsPropertyHelper<T>;
export function ToProperty<T>(source: Observable<T>, owner: PropertyNotificationTarget, propertyName: string, initialValue?: T, options?: ObservableAsPropertyOptions<T>): ObservableAsPropertyHelper<T>;
export function ToProperty<T>(source: Observable<T>, owner: PropertyNotificationTarget, propertyName: string, initialValueOrOptions?: T | ObservableAsPropertyOptions<T>, extraOptions?: ObservableAsPropertyOptions<T>): ObservableAsPropertyHelper<T> {
  const options: ObservableAsPropertyOptions<T> = extraOptions
    ? { ...extraOptions, initialValue: initialValueOrOptions as T }
    : isOptions<T>(initialValueOrOptions) ? initialValueOrOptions : { initialValue: initialValueOrOptions as T };
  const helper = new ObservableAsPropertyHelper(source, {
    ...options,
    deferSubscription: true,
    onChanging(value, previous) {
      owner.RaisePropertyChanging(propertyName, previous, value);
      options.onChanging?.(value, previous);
    },
    onChanged(value, previous) {
      owner.RaisePropertyChanged(propertyName, previous, value);
      options.onChanged?.(value, previous);
    }
  });
  let current: object | null = owner;
  let descriptor: PropertyDescriptor | undefined;
  while (current && !descriptor) {
    descriptor = Object.getOwnPropertyDescriptor(current, propertyName);
    current = Object.getPrototypeOf(current);
  }
  const shouldInstall = options.installProperty ?? !(descriptor?.get && !descriptor.set);
  if (shouldInstall) {
    try {
      Object.defineProperty(owner, propertyName, { enumerable: true, configurable: true, get: () => helper.Value });
    } catch (error) { helper.Dispose(); throw error; }
  }
  if (!options.deferSubscription) helper.Subscribe();
  return helper;
}
export const toProperty = ToProperty;
