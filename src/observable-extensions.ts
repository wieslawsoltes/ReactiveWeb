import {
  Observable, catchError, filter, from, isObservable, of, switchMap, tap, throwError,
  type MonoTypeOperatorFunction, type ObservableInput, type Observer, type OperatorFunction,
} from 'rxjs';
import { RxApp } from './rx-app.js';
import type { DisposableSubscription } from './scheduled-subject.js';

/** Project non-null values and switch to the latest projected observable. */
export function SwitchSelect<T, R>(selector: (value: T) => ObservableInput<R>): OperatorFunction<T | null | undefined, R>;
export function SwitchSelect<T, R>(source: Observable<T | null | undefined>, selector: (value: T) => ObservableInput<R>): Observable<R>;
export function SwitchSelect<T, R>(
  sourceOrSelector: Observable<T | null | undefined> | ((value: T) => ObservableInput<R>),
  selector?: (value: T) => ObservableInput<R>,
): Observable<R> | OperatorFunction<T | null | undefined, R> {
  const project = selector ?? sourceOrSelector as (value: T) => ObservableInput<R>;
  if (typeof project !== 'function') throw new TypeError('A selector is required.');
  const operator: OperatorFunction<T | null | undefined, R> = source => source.pipe(
    filter((value): value is T => value != null),
    switchMap(project),
  );
  return selector ? operator(sourceOrSelector as Observable<T | null | undefined>) : operator;
}

type SubscriptionObserver<T> = Partial<Observer<T>> | ((value: T) => void);

/** Subscribe to a replaceable inner observable, ignoring null outer values. */
export function SwitchSubscribe<T>(source: Observable<ObservableInput<T> | null | undefined>, observer: SubscriptionObserver<T>): DisposableSubscription;
/** Project a replaceable object (such as a command), then subscribe to its stream. */
export function SwitchSubscribe<T, R>(source: Observable<T | null | undefined>, selector: (value: T) => ObservableInput<R>, observer: SubscriptionObserver<R>): DisposableSubscription;
export function SwitchSubscribe<T, R>(
  source: Observable<T | null | undefined>,
  selectorOrObserver: ((value: T) => ObservableInput<R>) | SubscriptionObserver<R>,
  observer?: SubscriptionObserver<R>,
): DisposableSubscription {
  const selector = observer === undefined ? (value: T) => value as ObservableInput<R> : selectorOrObserver as (value: T) => ObservableInput<R>;
  const target = observer ?? selectorOrObserver as SubscriptionObserver<R>;
  const handlers = typeof target === 'function' ? { next: target } : target;
  const subscription = SwitchSelect(source, selector).subscribe({
    next: value => handlers.next?.(value),
    error: error => handlers.error ? handlers.error(error) : RxApp.HandleException(error),
    complete: () => handlers.complete?.(),
  });
  return Object.assign(subscription, { Dispose() { subscription.unsubscribe(); } });
}

/** .NET-style Do is RxJS tap: callbacks run before the unchanged notification. */
export function Do<T>(observer: SubscriptionObserver<T>, onError?: (error: unknown) => void, onCompleted?: () => void): MonoTypeOperatorFunction<T>;
export function Do<T>(source: Observable<T>, observer: SubscriptionObserver<T>, onError?: (error: unknown) => void, onCompleted?: () => void): Observable<T>;
export function Do<T>(
  sourceOrObserver: Observable<T> | SubscriptionObserver<T>,
  observerOrError?: SubscriptionObserver<T> | ((error: unknown) => void),
  errorOrComplete?: ((error: unknown) => void) | (() => void),
  onCompleted?: () => void,
): Observable<T> | MonoTypeOperatorFunction<T> {
  const direct = isObservable(sourceOrObserver);
  const observer = (direct ? observerOrError : sourceOrObserver) as SubscriptionObserver<T>;
  const onError = (direct ? errorOrComplete : observerOrError) as ((error: unknown) => void) | undefined;
  const complete = (direct ? onCompleted : errorOrComplete) as (() => void) | undefined;
  const handlers = typeof observer === 'function' ? { next: observer, error: onError, complete } : observer;
  const operator = tap<T>(handlers);
  return direct ? (sourceOrObserver as Observable<T>).pipe(operator) : operator;
}

/** Logger contracts accept browser consoles and small .NET-style logger adapters. */
export interface ObservableLogger {
  info?(message: string, value?: unknown): void;
  warn?(message: string, error?: unknown): void;
  Info?(message: string, value?: unknown): void;
  Warn?(error: unknown, message: string): void;
}
export type ObservableLoggerSource = ObservableLogger | { Log(): ObservableLogger };

function resolveLogger(source: ObservableLoggerSource): ObservableLogger {
  return 'Log' in source && typeof source.Log === 'function' ? source.Log() : source as ObservableLogger;
}
function info(logger: ObservableLoggerSource, message: string, value?: unknown): void {
  const target = resolveLogger(logger);
  if (target.info) target.info(message, value);
  else target.Info?.(message, value);
}
function warn(logger: ObservableLoggerSource, message: string, error: unknown): void {
  const target = resolveLogger(logger);
  if (target.warn) target.warn(message, error);
  else target.Warn?.(error, message);
}

/** Log every notification while preserving values, errors, and completion. */
export function Log<T>(logger?: ObservableLoggerSource, message?: string, stringifier?: (value: T) => string): MonoTypeOperatorFunction<T>;
export function Log<T>(source: Observable<T>, logger?: ObservableLoggerSource, message?: string, stringifier?: (value: T) => string): Observable<T>;
export function Log<T>(
  sourceOrLogger: Observable<T> | ObservableLoggerSource = console,
  loggerOrMessage?: ObservableLoggerSource | string,
  messageOrStringifier?: string | ((value: T) => string),
  stringify?: (value: T) => string,
): Observable<T> | MonoTypeOperatorFunction<T> {
  const direct = isObservable(sourceOrLogger);
  const logger = (direct ? loggerOrMessage ?? console : sourceOrLogger) as ObservableLoggerSource;
  const message = (direct ? messageOrStringifier : loggerOrMessage) as string | undefined;
  const stringifier = (direct ? stringify : messageOrStringifier) as ((value: T) => string) | undefined;
  const prefix = message ? `${message} ` : '';
  const operator = tap<T>({
    next: value => info(logger, `${prefix}OnNext`, stringifier ? stringifier(value) : value),
    error: error => warn(logger, `${prefix}OnError`, error),
    complete: () => info(logger, `${prefix}OnCompleted`),
  });
  return direct ? (sourceOrLogger as Observable<T>).pipe(operator) : operator;
}

export type LoggedFallback<T> = ObservableInput<T> | ((error: unknown) => ObservableInput<T>);

/** Log a matching source error, then use its fallback. Omitted fallback emits undefined once. */
export function LoggedCatch<T>(logger?: ObservableLoggerSource): OperatorFunction<T, T | undefined>;
export function LoggedCatch<T>(logger: ObservableLoggerSource, fallback: LoggedFallback<T>, message?: string, shouldCatch?: (error: unknown) => boolean): MonoTypeOperatorFunction<T>;
export function LoggedCatch<T>(source: Observable<T>, logger?: ObservableLoggerSource): Observable<T | undefined>;
export function LoggedCatch<T>(source: Observable<T>, logger: ObservableLoggerSource, fallback: LoggedFallback<T>, message?: string, shouldCatch?: (error: unknown) => boolean): Observable<T>;
export function LoggedCatch<T>(
  sourceOrLogger: Observable<T> | ObservableLoggerSource = console,
  loggerOrFallback?: ObservableLoggerSource | LoggedFallback<T>,
  fallbackOrMessage?: LoggedFallback<T> | string,
  messageOrPredicate?: string | ((error: unknown) => boolean),
  predicate?: (error: unknown) => boolean,
): Observable<T | undefined> | OperatorFunction<T, T | undefined> {
  const direct = isObservable(sourceOrLogger);
  const logger = (direct ? loggerOrFallback ?? console : sourceOrLogger) as ObservableLoggerSource;
  const fallback = (direct ? fallbackOrMessage : loggerOrFallback) as LoggedFallback<T> | undefined;
  const message = (direct ? messageOrPredicate : fallbackOrMessage) as string | undefined;
  const shouldCatch = (direct ? predicate : messageOrPredicate) as ((error: unknown) => boolean) | undefined;
  const operator: OperatorFunction<T, T | undefined> = source => source.pipe(catchError(error => {
    if (shouldCatch && !shouldCatch(error)) return throwError(() => error);
    warn(logger, message ?? '', error);
    if (fallback === undefined) return of(undefined);
    return from(typeof fallback === 'function' ? fallback(error) : fallback);
  }));
  return direct ? operator(sourceOrLogger as Observable<T>) : operator;
}

export const switchSelect = SwitchSelect;
export const switchSubscribe = SwitchSubscribe;
export const loggedCatch = LoggedCatch;
export const logObservable = Log;
export const ObservableLoggingMixins = { Log, LoggedCatch, Do };
export const SwitchSubscribeMixins = { SwitchSelect, SwitchSubscribe };
