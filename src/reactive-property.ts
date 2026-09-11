import { BehaviorSubject, Observable, Subscription, combineLatest, defaultIfEmpty, from, isObservable, observable, observeOn, of, skip, type Observer, type Operator, type OperatorFunction, type PartialObserver, type SchedulerLike } from 'rxjs';
import { CompositeDisposable, Disposable, type IDisposable } from './disposables.js';
import { ReactiveObject } from './reactive-object.js';

export type ReactiveValueValidationResult = string | readonly string[] | null | undefined | boolean;
export type ReactivePropertyValidator<T> = (value: T) => ReactiveValueValidationResult | PromiseLike<ReactiveValueValidationResult> | Observable<ReactiveValueValidationResult>;
export interface ReactiveValueOptions<T> {
  initialValue?: T;
  allowDuplicateValues?: boolean;
  skipCurrentValueOnSubscribe?: boolean;
  ignoreInitialError?: boolean;
  comparer?: (previous: T, current: T) => boolean;
  scheduler?: SchedulerLike;
}

function errorsFrom(result: ReactiveValueValidationResult): readonly string[] {
  if (result == null || result === true) return [];
  if (result === false) return ['The value is invalid.'];
  return typeof result === 'string' ? result ? [result] : [] : result.filter(message => !!message);
}
function isPropertyOptions<T>(value: unknown): value is ReactiveValueOptions<T> {
  return value !== null && typeof value === 'object' &&
    (Object.keys(value).length === 0 || ['initialValue', 'allowDuplicateValues', 'skipCurrentValueOnSubscribe', 'ignoreInitialError', 'comparer', 'scheduler'].some(key => key in value));
}

/** A mutable reactive value, an RxJS source, and a cancellable validation model. */
export class ReactiveProperty<T> extends ReactiveObject implements Observer<T> {
  private currentValue: T;
  private readonly valueSubject: BehaviorSubject<T>;
  private readonly validationValueSubject: BehaviorSubject<T>;
  private readonly errorSubject = new BehaviorSubject<readonly string[]>([]);
  private readonly hasErrorsSubject = new BehaviorSubject(false);
  private readonly validatingSubject = new BehaviorSubject(false);
  private readonly errorsChangedSubject = new BehaviorSubject({ PropertyName: 'Value', propertyName: 'Value' });
  private readonly sourceSubscription = new Subscription();
  private validationSubscription = new Subscription();
  private validationVersion = 0;
  private readonly validators = new Set<ReactivePropertyValidator<T>>();
  private readonly streamErrors = new Map<object, readonly string[]>();
  private valueErrors: readonly string[] = [];
  private readonly options: ReactiveValueOptions<T>;
  private wasEdited = false;
  readonly ErrorsChanged = this.errorsChangedSubject.asObservable();
  readonly IsValidatingObservable = this.validatingSubject.asObservable();

  static Create<T>(initialValue?: T, options?: ReactiveValueOptions<T>): ReactiveProperty<T> { return new ReactiveProperty(initialValue, options); }

  constructor(initialValue?: T, options?: ReactiveValueOptions<T>);
  constructor(source: Observable<T>, options?: ReactiveValueOptions<T>);
  constructor(source: Observable<T>, initialValue: T, options?: ReactiveValueOptions<T>);
  constructor(initialValueOrSource?: T | Observable<T>, optionsOrInitial?: ReactiveValueOptions<T> | T, sourceOptions?: ReactiveValueOptions<T>) {
    super();
    const source = isObservable(initialValueOrSource) ? initialValueOrSource as Observable<T> : undefined;
    this.options = source
      ? sourceOptions ? { ...sourceOptions, initialValue: optionsOrInitial as T }
        : isPropertyOptions<T>(optionsOrInitial) ? optionsOrInitial : { initialValue: optionsOrInitial as T }
      : (optionsOrInitial ?? {}) as ReactiveValueOptions<T>;
    this.currentValue = source ? this.options.initialValue as T : initialValueOrSource as T;
    this.valueSubject = new BehaviorSubject(this.currentValue);
    this.validationValueSubject = new BehaviorSubject(this.currentValue);
    if (source) this.sourceSubscription.add(source.subscribe({ next: value => this.OnNext(value), error: error => this.OnError(error) }));
  }
  get Value(): T { return this.currentValue; }
  set Value(value: T) { this.OnNext(value); }
  get value(): T { return this.Value; }
  set value(value: T) { this.Value = value; }
  get HasErrors(): boolean { return this.errorSubject.value.length > 0; }
  get IsValidating(): boolean { return this.validatingSubject.value; }
  get Errors(): readonly string[] { return this.errorSubject.value; }
  get ObserveErrorChanged(): Observable<readonly string[]> { return this.errorSubject.asObservable(); }
  get ObserveHasErrors(): Observable<boolean> { return this.hasErrorsSubject.asObservable(); }
  observeErrorChanged(): Observable<readonly string[]> { return this.ObserveErrorChanged; }
  observeHasErrors(): Observable<boolean> { return this.ObserveHasErrors; }
  GetErrors(propertyName?: string | null): readonly string[] { return propertyName && propertyName !== 'Value' ? [] : this.Errors; }

  OnNext(value: T): void {
    if (this.IsDisposed) return;
    this.wasEdited = true;
    if (!this.options.allowDuplicateValues && (this.options.comparer ?? Object.is)(this.currentValue, value)) return;
    this.setCurrent(value);
  }
  private setCurrent(value: T): void {
    const previous = this.currentValue;
    this.RaisePropertyChanging('Value', previous, value);
    this.currentValue = value;
    this.RaisePropertyChanged('Value', previous, value);
    this.valueSubject.next(value);
    this.CheckValidation();
  }
  OnError(error: unknown): void { if (!this.IsDisposed) this.ReportException(error); }
  OnCompleted(): void { this.valueSubject.complete(); }
  next(value: T): void { this.OnNext(value); }
  error(error: unknown): void { this.OnError(error); }
  complete(): void { this.OnCompleted(); }
  Refresh(): void {
    if (this.IsDisposed) return;
    this.wasEdited = true;
    this.setCurrent(this.currentValue);
  }
  refresh(): void { this.Refresh(); }

  AddValidationError(validator: ReactivePropertyValidator<T>, ignoreInitialError = this.options.ignoreInitialError ?? false): this {
    if (this.IsDisposed) throw new Error('ReactiveProperty has been disposed.');
    this.validators.add(validator);
    if (!ignoreInitialError || this.wasEdited) this.CheckValidation();
    return this;
  }
  /** Persistent stream validators retain operator state, including debounce, switchMap, and scan. */
  AddValidationErrorObservable(transform: (values: Observable<T>) => Observable<ReactiveValueValidationResult>, ignoreInitialError = this.options.ignoreInitialError ?? false): this {
    if (this.IsDisposed) throw new Error('ReactiveProperty has been disposed.');
    const key = {};
    this.streamErrors.set(key, []);
    const values = ignoreInitialError ? this.validationValueSubject.pipe(skip(1)) : this.validationValueSubject.asObservable();
    try {
      this.sourceSubscription.add(transform(values).subscribe({
        next: errors => { this.streamErrors.set(key, errorsFrom(errors)); this.publishErrors(); },
        error: error => { this.streamErrors.set(key, [error instanceof Error ? error.message : String(error)]); this.publishErrors(); }
      }));
    } catch (error) {
      this.streamErrors.set(key, [error instanceof Error ? error.message : String(error)]);
      this.publishErrors();
    }
    return this;
  }
  /** Web convenience: returns a removable validator lifetime. */
  AddValidator(validator: ReactivePropertyValidator<T>): IDisposable & { unsubscribe(): void } {
    this.AddValidationError(validator);
    return Disposable.Create(() => {
      this.validators.delete(validator);
      this.CheckValidation();
    });
  }
  CheckValidation(): void {
    if (this.IsDisposed) return;
    const version = ++this.validationVersion;
    this.validationSubscription.unsubscribe();
    this.validationSubscription = new Subscription();
    this.validationValueSubject.next(this.currentValue);
    if (version !== this.validationVersion || this.IsDisposed) return;
    const validations: Observable<readonly string[]>[] = [];
    for (const validator of this.validators) {
      try {
        const result = validator(this.currentValue);
        const observableResult = isObservable(result) ? result
          : result && typeof (result as PromiseLike<unknown>).then === 'function'
            ? from(result as PromiseLike<ReactiveValueValidationResult>) : of(result as ReactiveValueValidationResult);
        validations.push(new Observable<readonly string[]>(subscriber => observableResult.subscribe({
          next: value => subscriber.next(errorsFrom(value)),
          error: error => { subscriber.next([error instanceof Error ? error.message : String(error)]); subscriber.complete(); },
          complete: () => subscriber.complete()
        })).pipe(defaultIfEmpty([])));
      } catch (error) { validations.push(of([error instanceof Error ? error.message : String(error)])); }
    }
    if (!validations.length) { this.valueErrors = []; this.publishErrors(); this.setValidating(false); return; }
    this.setValidating(true);
    const subscription = combineLatest(validations).subscribe({
      next: results => {
        if (version !== this.validationVersion || this.IsDisposed) return;
        this.valueErrors = results.flat();
        this.publishErrors();
        if (version === this.validationVersion) this.setValidating(false);
      },
      complete: () => { if (version === this.validationVersion && !this.IsDisposed) this.setValidating(false); }
    });
    this.validationSubscription.add(subscription);
  }
  private publishErrors(): void { this.setErrors([...new Set([...this.valueErrors, ...[...this.streamErrors.values()].flat()])]); }
  private setValidating(value: boolean): void {
    if (this.validatingSubject.value === value) return;
    this.RaisePropertyChanging('IsValidating', !value, value);
    this.validatingSubject.next(value);
    this.RaisePropertyChanged('IsValidating', !value, value);
  }
  private setErrors(errors: readonly string[]): void {
    const previous = this.errorSubject.value;
    if (previous.length === errors.length && previous.every((value, index) => value === errors[index])) return;
    const hadErrors = previous.length > 0;
    const hasErrors = errors.length > 0;
    this.RaisePropertyChanging('Errors', previous, errors);
    if (hadErrors !== hasErrors) this.RaisePropertyChanging('HasErrors', hadErrors, hasErrors);
    this.errorSubject.next(Object.freeze([...errors]));
    if (hadErrors !== hasErrors) this.hasErrorsSubject.next(hasErrors);
    this.errorsChangedSubject.next({ PropertyName: 'Value', propertyName: 'Value' });
    this.RaisePropertyChanged('Errors', previous, errors);
    if (hadErrors !== hasErrors) this.RaisePropertyChanged('HasErrors', hadErrors, hasErrors);
  }
  asObservable(): Observable<T> {
    const source = this.valueSubject.asObservable();
    const selected = this.options.skipCurrentValueOnSubscribe ? source.pipe(skip(1)) : source;
    return this.options.scheduler ? selected.pipe(observeOn(this.options.scheduler)) : selected;
  }
  ToObservable(): Observable<T> { return this.asObservable(); }
  [observable](): Observable<T> { return this.asObservable(); }
  lift<R>(operator?: Operator<T, R>): Observable<R> { return this.asObservable().lift(operator); }
  subscribe(observer?: PartialObserver<T> | ((value: T) => void)): Subscription { return this.asObservable().subscribe(observer); }
  Subscribe(observer?: PartialObserver<T> | ((value: T) => void)): Subscription { return this.subscribe(observer); }
  pipe(): Observable<T>;
  pipe<A>(op1: OperatorFunction<T, A>): Observable<A>;
  pipe<A, B>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>): Observable<B>;
  pipe<A, B, C>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>): Observable<C>;
  pipe(...operators: OperatorFunction<any, any>[]): Observable<any>;
  pipe(...operators: OperatorFunction<any, any>[]): Observable<any> { return operators.reduce((source, operator) => operator(source), this.asObservable() as Observable<any>); }
  override Dispose(): void {
    if (this.IsDisposed) return;
    this.validationVersion++;
    super.Dispose();
    try { new CompositeDisposable(this.validationSubscription, this.sourceSubscription).Dispose(); }
    finally {
      this.validators.clear();
      this.streamErrors.clear();
      this.valueSubject.complete();
      this.validationValueSubject.complete();
      this.errorSubject.complete();
      this.hasErrorsSubject.complete();
      this.validatingSubject.complete();
      this.errorsChangedSubject.complete();
    }
  }
}

export function ToReactiveProperty<T>(source: Observable<T>, options?: ReactiveValueOptions<T>): ReactiveProperty<T> {
  return new ReactiveProperty(source, options);
}
export const toReactiveProperty = ToReactiveProperty;
