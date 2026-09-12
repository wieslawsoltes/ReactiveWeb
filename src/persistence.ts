import { BehaviorSubject, Observable, ReplaySubject, Subject, Subscription, catchError, concatMap, debounceTime, defer, EMPTY, from, isObservable, map, merge, of, tap, type SchedulerLike } from 'rxjs';
import { Disposable, type IDisposable } from './disposables.js';
import { ActOnEveryObject } from './collections.js';
import type { DynamicDataSource } from './dynamic-data.js';

export type AsyncResult<T> = T | PromiseLike<T> | Observable<T>;
export interface ISuspensionDriver<T = unknown> {
  LoadState(): AsyncResult<T | null>;
  SaveState(state: T): AsyncResult<unknown>;
  InvalidateState(): AsyncResult<unknown>;
}
function observableResult<T>(action: () => AsyncResult<T>): Observable<T> {
  return defer(() => { const value = action(); return isObservable(value) ? value : value && typeof (value as PromiseLike<T>).then === 'function' ? from(value as PromiseLike<T>) : of(value as T); });
}
function clone<T>(value: T): T { return typeof structuredClone === 'function' ? structuredClone(value) : JSON.parse(JSON.stringify(value)) as T; }
export class InMemorySuspensionDriver<T = unknown> implements ISuspensionDriver<T> {
  private state: T | null = null;
  LoadState(): T | null { return this.state === null ? null : clone(this.state); }
  SaveState(state: T): void { this.state = clone(state); }
  InvalidateState(): void { this.state = null; }
}
export interface StorageLike { getItem(key: string): string | null; setItem(key: string, value: string): void; removeItem(key: string): void; }
export interface JsonPersistenceOptions<T> {
  key?: string;
  storage?: StorageLike;
  /** Validate or migrate parsed data before the application receives it. */
  deserialize?: (json: unknown) => T;
  serialize?: (state: T) => unknown;
}
export class LocalStorageSuspensionDriver<T = unknown> implements ISuspensionDriver<T> {
  private readonly options: JsonPersistenceOptions<T>;
  private readonly storage: StorageLike;
  constructor(options: JsonPersistenceOptions<T> | string = {}) {
    this.options = typeof options === 'string' ? { key: options } : options;
    const storage = this.options.storage ?? globalThis.localStorage;
    if (!storage) throw new Error('localStorage is unavailable; supply a StorageLike implementation');
    this.storage = storage;
  }
  get Key(): string { return this.options.key ?? 'reactiveweb.state'; }
  LoadState(): T | null {
    const text = this.storage.getItem(this.Key); if (text === null) return null;
    const json: unknown = JSON.parse(text); return this.options.deserialize ? this.options.deserialize(json) : json as T;
  }
  SaveState(state: T): void {
    const text = JSON.stringify(this.options.serialize ? this.options.serialize(state) : state);
    if (text === undefined) throw new TypeError('State is not JSON serializable');
    this.storage.setItem(this.Key, text);
  }
  InvalidateState(): void { this.storage.removeItem(this.Key); }
}
export { LocalStorageSuspensionDriver as BrowserSuspensionDriver };

/** Coordinates launch/resume/persist lifecycle streams with a pluggable state driver. */
export class SuspensionHost<T = unknown> implements IDisposable {
  readonly IsLaunchingNew = new Subject<void>();
  readonly IsResuming = new Subject<void>();
  readonly IsUnpausing = new Subject<void>();
  readonly ShouldPersistState = new Subject<IDisposable | void>();
  readonly ShouldInvalidateState = new Subject<void>();
  readonly ThrownExceptions = new Subject<unknown>();
  CreateNewAppState: () => T;
  private readonly state = new BehaviorSubject<T | null>(null);
  private setup?: IDisposable;
  private disposed = false;
  constructor(createNewAppState: () => T = () => { throw new Error('CreateNewAppState must be configured'); }) { this.CreateNewAppState = createNewAppState; }
  get AppState(): T | null { return this.state.value; }
  set AppState(value: T | null) { if (this.disposed) throw new Error('Suspension host is disposed'); this.state.next(value); }
  get AppStateChanged(): Observable<T | null> { return this.state.asObservable(); }
  SetupDefaultSuspendResume(driver: ISuspensionDriver<T>): IDisposable {
    if (this.disposed) throw new Error('Suspension host is disposed');
    this.setup?.Dispose(); const subscriptions = new Subscription();
    const report = (error: unknown) => { this.ThrownExceptions.next(error); return EMPTY; };
    subscriptions.add(this.IsLaunchingNew.subscribe(() => { try { this.AppState = this.CreateNewAppState(); } catch (error) { report(error); } }));
    subscriptions.add(this.IsResuming.pipe(concatMap(() => observableResult(() => driver.LoadState()).pipe(
      catchError(error => { report(error); return of(null); })
    ))).subscribe(state => { try { this.AppState = state ?? this.CreateNewAppState(); } catch (error) { report(error); } }));
    const pendingLeases = new Set<IDisposable>();
    const persistRequests = this.ShouldPersistState.pipe(map(lease => ({ kind: 'persist' as const, lease })));
    const invalidateRequests = this.ShouldInvalidateState.pipe(map(() => ({ kind: 'invalidate' as const })));
    subscriptions.add(merge(persistRequests, invalidateRequests).pipe(
      tap(request => { if (request.kind === 'persist' && request.lease) pendingLeases.add(request.lease); }),
      concatMap(request => {
        if (request.kind === 'invalidate') return observableResult(() => driver.InvalidateState()).pipe(tap(() => { this.AppState = null; }), catchError(report));
        return new Observable<void>(subscriber => {
          const state = this.AppState;
          const saving = (state === null ? of(undefined) : observableResult(() => driver.SaveState(state))).subscribe({
            error: error => { report(error); subscriber.complete(); }, complete: () => subscriber.complete(),
          });
          return () => { saving.unsubscribe(); if (request.lease) { pendingLeases.delete(request.lease); request.lease.Dispose(); } };
        });
      })
    ).subscribe());
    subscriptions.add(() => { for (const lease of pendingLeases) { try { lease.Dispose(); } catch (error) { report(error); } } pendingLeases.clear(); });
    const setup = Disposable.Create(() => subscriptions.unsubscribe()); this.setup = setup; return setup;
  }
  GetAppState<TState extends T>(): TState | null { return this.AppState as TState | null; }
  Dispose(): void {
    if (this.disposed) return; this.disposed = true; this.setup?.Dispose();
    this.IsLaunchingNew.complete(); this.IsResuming.complete(); this.IsUnpausing.complete();
    this.ShouldPersistState.complete(); this.ShouldInvalidateState.complete(); this.ThrownExceptions.complete(); this.state.complete();
  }
  unsubscribe(): void { this.Dispose(); }
}
export function SetupDefaultSuspendResume<T>(host: SuspensionHost<T>, driver: ISuspensionDriver<T>): IDisposable { return host.SetupDefaultSuspendResume(driver); }

export interface AutoPersistOptions {
  throttleMs?: number;
  scheduler?: SchedulerLike;
  initial?: boolean;
  onError?: (error: unknown) => void;
  changes?: Observable<unknown>;
}
export interface IPersistenceSubscription extends IDisposable {
  readonly Errors: Observable<unknown>;
  Flush(): Promise<void>;
  Trigger(): void;
  unsubscribe(): void;
}

/** Debounced, serialized persistence. Flush surfaces failures; later changes retry normally. */
export function AutoPersist<T>(item: T, persist: (item: T) => AsyncResult<unknown>, options: AutoPersistOptions = {}): IPersistenceSubscription {
  const source = options.changes ?? (item as { Changed?: Observable<unknown> } | null)?.Changed;
  if (!source || typeof source.subscribe !== 'function') throw new TypeError('AutoPersist requires a Changed observable or options.changes');
  if (options.throttleMs !== undefined && (!Number.isFinite(options.throttleMs) || options.throttleMs < 0)) throw new RangeError('throttleMs must be a finite nonnegative number');
  const subscriptions = new Subscription(), requests = new Subject<void>(), errors = new Subject<unknown>();
  let disposed = false, revision = 0, requestedRevision = 0;
  let queue: Promise<void> = Promise.resolve();
  const report = (error: unknown) => { errors.next(error); options.onError?.(error); };
  const flush = (): Promise<void> => {
    if (disposed || revision === requestedRevision) return queue;
    requestedRevision = revision;
    const job = queue.catch(() => undefined).then(async () => {
      if (disposed) return;
      try {
        await new Promise<void>((resolve, reject) => {
          const savingScope = new Subscription(() => resolve()); subscriptions.add(savingScope);
          savingScope.add(observableResult(() => persist(item)).subscribe({
            error: error => { reject(error); savingScope.unsubscribe(); subscriptions.remove(savingScope); },
            complete: () => { resolve(); savingScope.unsubscribe(); subscriptions.remove(savingScope); },
          }));
        });
      } catch (error) { if (!disposed) report(error); throw error; }
    });
    // Keep the failure observable to explicit Flush callers while avoiding unhandled timer rejections.
    queue = job; void job.catch(() => undefined); return job;
  };
  const trigger = () => { if (!disposed) { revision++; requests.next(); } };
  subscriptions.add(requests.pipe(debounceTime(options.throttleMs ?? 1000, options.scheduler)).subscribe(() => { void flush().catch(() => undefined); }));
  subscriptions.add(source.subscribe({ next: trigger, error: report }));
  if (options.initial) trigger();
  return {
    Errors: errors.asObservable(), Flush: flush, Trigger: trigger,
    Dispose() { if (disposed) return; disposed = true; subscriptions.unsubscribe(); requests.complete(); errors.complete(); },
    unsubscribe() { this.Dispose(); },
  };
}

/** Browser attachment reports lifecycle only; browsers do not await arbitrary asynchronous unload saves. */
export function AttachBrowserLifecycle<T>(host: SuspensionHost<T>, target: Window = window): IDisposable {
  const onPageHide = () => host.ShouldPersistState.next();
  const onVisibility = () => { if (target.document.visibilityState === 'hidden') host.ShouldPersistState.next(); else host.IsUnpausing.next(); };
  const onPageShow = (event: PageTransitionEvent) => { if (event.persisted) host.IsUnpausing.next(); };
  target.addEventListener('pagehide', onPageHide); target.addEventListener('pageshow', onPageShow); target.document.addEventListener('visibilitychange', onVisibility);
  return Disposable.Create(() => { target.removeEventListener('pagehide', onPageHide); target.removeEventListener('pageshow', onPageShow); target.document.removeEventListener('visibilitychange', onVisibility); });
}

/** Persists each live object independently, and releases its observer when it leaves the collection. */
export function AutoPersistCollection<T>(collection: DynamicDataSource<T>, persist: (item: T) => AsyncResult<unknown>, options: AutoPersistOptions = {}): IPersistenceSubscription {
  const handles = new Map<T, IPersistenceSubscription>(), errors = new ReplaySubject<unknown>(1);
  let disposed = false, lifecycleError: unknown;
  const observer = ActOnEveryObject(collection, item => {
    const handle = AutoPersist(item, persist, options); handles.set(item, handle);
    const errorsSubscription = handle.Errors.subscribe(error => errors.next(error));
    return Disposable.Create(() => { errorsSubscription.unsubscribe(); handle.Dispose(); handles.delete(item); });
  });
  const lifecycleErrors = observer.Errors.subscribe(error => { lifecycleError = error; errors.next(error); options.onError?.(error); });
  return {
    Errors: errors.asObservable(),
    async Flush() { if (!disposed) { await Promise.all([...handles.values()].map(handle => handle.Flush())); if (lifecycleError !== undefined) throw lifecycleError; } },
    Trigger() { if (!disposed) for (const handle of handles.values()) handle.Trigger(); },
    Dispose() { if (disposed) return; disposed = true; try { observer.Dispose(); } finally { lifecycleErrors.unsubscribe(); errors.complete(); } },
    unsubscribe() { this.Dispose(); },
  };
}
