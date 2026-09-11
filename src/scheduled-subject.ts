import {
  Observable, Subject, Subscriber, Subscription, asyncScheduler, observeOn, queueScheduler,
  type Observer, type SchedulerAction, type SchedulerLike,
} from 'rxjs';
import { RxApp } from './rx-app.js';

export type DisposableSubscription = Subscription & { Dispose(): void };

/** A subject whose observers receive notifications on a supplied scheduler. */
export class ScheduledSubject<T> extends Observable<T> implements Observer<T> {
  private readonly backing: Subject<T>;
  private readonly subscriptions = new Subscription();
  private fallbackSubscription: Subscription | undefined;
  private observerCount = 0;
  private disposed = false;
  private terminal = false;

  constructor(
    private readonly scheduler: SchedulerLike = RxApp.MainThreadScheduler,
    private readonly defaultObserver?: Partial<Observer<T>> | ((value: T) => void),
    defaultSubject?: Subject<T>,
  ) {
    super(subscriber => this.subscribeObserver(subscriber));
    this.backing = defaultSubject ?? new Subject<T>();
    this.attachDefaultObserver();
  }

  private subscribeObserver(observer: Subscriber<T>): Subscription | undefined {
    if (this.IsDisposed) { observer.error(new Error('ScheduledSubject has been disposed.')); return; }
    this.fallbackSubscription?.unsubscribe();
    this.fallbackSubscription = undefined;
    this.observerCount++;
    observer.add(() => {
      this.observerCount--;
      if (this.observerCount === 0) this.attachDefaultObserver();
    });
    this.subscriptions.add(observer);
    return this.backing.pipe(observeOn(this.scheduler)).subscribe(observer);
  }

  private attachDefaultObserver(): void {
    if (this.IsDisposed || this.terminal || this.backing.isStopped || !this.defaultObserver || this.observerCount) return;
    const observer = typeof this.defaultObserver === 'function'
      ? { next: this.defaultObserver }
      : this.defaultObserver;
    this.fallbackSubscription = this.backing.pipe(observeOn(this.scheduler)).subscribe({
      next: value => observer.next?.(value),
      error: error => observer.error ? observer.error(error) : RxApp.HandleException(error),
      complete: () => observer.complete?.(),
    });
  }

  get HasObservers(): boolean { return !this.IsDisposed && this.backing.observed; }
  get ObserverCount(): number { return this.observerCount; }
  get IsDisposed(): boolean { return this.disposed || this.backing.closed; }
  get closed(): boolean { return this.IsDisposed; }
  get observed(): boolean { return this.HasObservers; }

  OnNext(value: T): void { this.next(value); }
  OnError(error: unknown): void { this.error(error); }
  OnCompleted(): void { this.complete(); }
  next(value: T): void {
    if (this.IsDisposed) throw new Error('ScheduledSubject has been disposed.');
    this.backing.next(value);
  }
  error(error: unknown): void {
    if (this.IsDisposed) throw new Error('ScheduledSubject has been disposed.');
    this.terminal = true;
    this.backing.error(error);
  }
  complete(): void {
    if (this.IsDisposed) throw new Error('ScheduledSubject has been disposed.');
    this.terminal = true;
    this.backing.complete();
  }
  Subscribe(observer: Partial<Observer<T>> | ((value: T) => void)): DisposableSubscription {
    const subscription = this.subscribe(observer);
    return Object.assign(subscription, { Dispose() { subscription.unsubscribe(); } });
  }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.fallbackSubscription?.unsubscribe();
    this.subscriptions.unsubscribe();
    this.backing.unsubscribe();
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}

export interface DispatcherSchedulerOptions {
  /** Default false: use fallback now and retry the provider on the next request. */
  waitForDispatcher?: boolean;
  /** Scheduler used before the provider becomes available. Defaults to queueScheduler. */
  fallbackScheduler?: SchedulerLike;
  /** Scheduler that times retries in wait mode. Defaults to asyncScheduler. */
  retryScheduler?: SchedulerLike;
  /** A positive retry delay in milliseconds; defaults to 16. */
  retryDelay?: number;
}

/**
 * Retries a lazily available dispatcher and caches the first successful result.
 * Default behavior matches ReactiveUI: unavailable work runs on the fallback.
 * The web-only wait mode retries through delayed, cancellable scheduler actions.
 */
export class WaitForDispatcherScheduler implements SchedulerLike {
  private dispatcher?: SchedulerLike;
  private readonly pending = new Subscription();
  private disposed = false;
  private lastError: unknown;
  private readonly fallback: SchedulerLike;
  private readonly retryScheduler: SchedulerLike;
  private readonly retryDelay: number;
  private readonly wait: boolean;

  constructor(
    private readonly provider: () => SchedulerLike | undefined | null = () => RxApp.MainThreadScheduler,
    options: DispatcherSchedulerOptions = {},
  ) {
    if (typeof provider !== 'function') throw new TypeError('A scheduler provider is required.');
    this.fallback = options.fallbackScheduler ?? queueScheduler;
    this.retryScheduler = options.retryScheduler ?? asyncScheduler;
    this.retryDelay = options.retryDelay ?? 16;
    if (!Number.isFinite(this.retryDelay) || this.retryDelay <= 0) throw new RangeError('retryDelay must be positive.');
    this.wait = options.waitForDispatcher ?? false;
    this.resolve();
  }

  private resolve(): SchedulerLike | undefined {
    if (this.disposed) return undefined;
    if (this.dispatcher) return this.dispatcher;
    try {
      const scheduler = this.provider();
      if (scheduler != null) {
        if (scheduler === this || typeof scheduler.schedule !== 'function' || typeof scheduler.now !== 'function') {
          throw new TypeError('The dispatcher provider must return another SchedulerLike.');
        }
        this.dispatcher = scheduler;
        this.lastError = undefined;
      }
    } catch (error) { this.lastError = error; }
    return this.dispatcher;
  }

  now(): number { return (this.resolve() ?? this.fallback).now(); }
  get Now(): number { return this.now(); }
  get IsReady(): boolean { return this.dispatcher !== undefined; }
  get IsDisposed(): boolean { return this.disposed; }
  get LastProviderError(): unknown { return this.lastError; }

  schedule<T>(work: (this: SchedulerAction<T>, state: T) => void, delay = 0, state?: T): Subscription {
    if (this.disposed) throw new Error('WaitForDispatcherScheduler has been disposed.');
    const scheduler = this;
    let current: Subscription | undefined;
    const action = new Subscription(() => current?.unsubscribe()) as SchedulerAction<T>;
    action.schedule = (nextState?: T, nextDelay = 0): Subscription => {
      if (action.closed || scheduler.disposed) return action;
      scheduler.pending.add(action);
      current?.unsubscribe();
      const cycle = new Subscription();
      current = cycle;
      const deadline = scheduler.retryScheduler.now() + Math.max(0, nextDelay);
      const attempt = () => {
        if (action.closed || cycle.closed) return;
        const dispatcher = scheduler.resolve() ?? (scheduler.wait ? undefined : scheduler.fallback);
        if (dispatcher) {
          const remaining = Math.max(0, deadline - scheduler.retryScheduler.now());
          cycle.add(dispatcher.schedule(() => {
            if (action.closed || cycle.closed) return;
            try { work.call(action, nextState as T); }
            catch (error) { action.unsubscribe(); throw error; }
            finally {
              // Completed one-shot actions must not accumulate on a long-lived
              // dispatcher. A recursively rescheduled action owns a new cycle.
              if (current === cycle) {
                cycle.unsubscribe();
                scheduler.pending.remove(action);
              }
            }
          }, remaining));
        } else {
          cycle.add(scheduler.retryScheduler.schedule(function () {
            attempt();
            this.unsubscribe();
          }, scheduler.retryDelay));
        }
      };
      attempt();
      return action;
    };
    action.schedule(state, delay);
    return action;
  }

  Schedule<T>(state: T, action: (scheduler: this, state: T) => void, delay = 0): DisposableSubscription {
    const subscription = this.schedule(() => action(this, state), delay);
    return Object.assign(subscription, { Dispose() { subscription.unsubscribe(); } });
  }
  Dispose(): void { if (!this.disposed) { this.disposed = true; this.pending.unsubscribe(); } }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}
