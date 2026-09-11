import { BehaviorSubject, Observable, Subscription, catchError, defer, distinctUntilChanged, from, isObservable, map, of, startWith, switchMap } from 'rxjs';
import { ReactiveObject, WhenAnyValue } from './reactive-object.js';
import { Disposable, type IDisposable } from './disposables.js';

export interface ValidationState {
  readonly IsValid: boolean;
  readonly Text: readonly string[];
  readonly IsPending: boolean;
}
export type ValidationResult = boolean | string | readonly string[] | ValidationState;
export type ValidationPredicate<T> = (value: T) => ValidationResult | PromiseLike<ValidationResult> | Observable<ValidationResult>;
export interface IValidatableViewModel { readonly ValidationContext: ValidationContext }
export interface ValidationComponent extends IDisposable {
  readonly PropertyName: string;
  readonly ValidationStatusChange: Observable<ValidationState>;
  readonly State: ValidationState;
}
const valid: ValidationState = Object.freeze({ IsValid: true, Text: Object.freeze([]), IsPending: false });
const pending: ValidationState = Object.freeze({ IsValid: false, Text: Object.freeze([]), IsPending: true });

/** Aggregates live rules. Pending validation deliberately prevents submission. */
export class ValidationContext implements IDisposable {
  private readonly rules = new Map<ValidationComponent, Subscription>();
  private readonly state = new BehaviorSubject<ValidationState>(valid);
  private disposed = false;
  readonly ValidationStatusChange = this.state.asObservable();
  readonly IsValid = this.ValidationStatusChange.pipe(map(x => x.IsValid), distinctUntilChanged());
  readonly IsPending = this.ValidationStatusChange.pipe(map(x => x.IsPending), distinctUntilChanged());
  readonly Text = this.ValidationStatusChange.pipe(map(x => x.Text));
  get State(): ValidationState { return this.state.value; }
  get IsValidValue(): boolean { return this.State.IsValid; }
  get IsPendingValue(): boolean { return this.State.IsPending; }
  get Validations(): readonly ValidationComponent[] { return [...this.rules.keys()]; }
  get HasErrors(): boolean { return !this.State.IsValid; }

  Add(rule: ValidationComponent): IDisposable {
    if (this.disposed) throw new Error('ValidationContext is disposed.');
    if (this.rules.has(rule)) throw new Error('The validation rule is already registered.');
    this.rules.set(rule, new Subscription());
    const subscription = rule.ValidationStatusChange.subscribe(() => this.refresh());
    this.rules.set(rule, subscription);
    this.refresh();
    return Disposable.Create(() => this.Remove(rule));
  }
  Remove(rule: ValidationComponent): boolean {
    const subscription = this.rules.get(rule);
    if (!subscription) return false;
    this.rules.delete(rule);
    subscription.unsubscribe();
    this.refresh();
    return true;
  }
  GetErrors(propertyName?: string): readonly string[] {
    return [...this.rules.keys()].filter(rule => propertyName == null || rule.PropertyName === propertyName).flatMap(rule => [...rule.State.Text]);
  }
  ObserveErrors(propertyName?: string): Observable<readonly string[]> {
    return this.ValidationStatusChange.pipe(map(() => this.GetErrors(propertyName)), distinctUntilChanged((a, b) => a.length === b.length && a.every((x, i) => x === b[i])));
  }
  private refresh(): void {
    if (this.disposed) return;
    const states = [...this.rules.keys()].map(rule => rule.State);
    this.state.next(Object.freeze({ IsValid: states.every(x => x.IsValid && !x.IsPending), IsPending: states.some(x => x.IsPending), Text: Object.freeze(states.flatMap(x => [...x.Text])) }));
  }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    const entries = [...this.rules];
    this.rules.clear();
    for (const [rule, subscription] of entries) { subscription.unsubscribe(); rule.Dispose(); }
    this.state.complete();
  }
  unsubscribe(): void { this.Dispose(); }
}

function normalize(result: ValidationResult, message: string): ValidationState {
  if (typeof result === 'boolean') return result ? valid : Object.freeze({ IsValid: false, Text: Object.freeze([message]), IsPending: false });
  if (typeof result === 'string') return result.length ? Object.freeze({ IsValid: false, Text: Object.freeze([result]), IsPending: false }) : valid;
  if (Array.isArray(result)) return result.length ? Object.freeze({ IsValid: false, Text: Object.freeze([...result]), IsPending: false }) : valid;
  const state = result as ValidationState;
  return Object.freeze({ IsValid: state.IsValid, IsPending: state.IsPending ?? false, Text: Object.freeze([...state.Text]) });
}

export class PropertyValidationRule<T = unknown> implements ValidationComponent {
  private readonly state = new BehaviorSubject<ValidationState>(pending);
  private readonly subscription: Subscription;
  private registration?: IDisposable;
  private disposed = false;
  readonly ValidationStatusChange = this.state.asObservable();
  get State(): ValidationState { return this.state.value; }
  constructor(context: ValidationContext, readonly PropertyName: string, values: Observable<T>, predicate: ValidationPredicate<T>, message = 'The value is invalid.') {
    this.registration = context.Add(this);
    this.subscription = values.pipe(switchMap(value => defer(() => {
      const result = predicate(value);
      const asynchronous = isObservable(result) || (result != null && typeof (result as PromiseLike<unknown>).then === 'function');
      const states = (asynchronous ? from(result as Observable<ValidationResult> | PromiseLike<ValidationResult>) : of(result as ValidationResult)).pipe(map(result => normalize(result, message)));
      return asynchronous ? states.pipe(startWith(pending)) : states;
    }).pipe(catchError(() => of(normalize(false, message)))))).subscribe(state => this.state.next(state));
  }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.subscription.unsubscribe();
    this.registration?.Dispose();
    this.registration = undefined;
    this.state.complete();
  }
  unsubscribe(): void { this.Dispose(); }
}

/** A .NET-style extension-function equivalent. Async rules cancel obsolete subscriptions. */
export function ValidationRule<T = unknown>(viewModel: object & IValidatableViewModel, propertyName: string, predicate: ValidationPredicate<T>, message?: string): PropertyValidationRule<T> {
  return new PropertyValidationRule(viewModel.ValidationContext, propertyName, WhenAnyValue(viewModel, propertyName) as Observable<T>, predicate, message);
}

export class ReactiveValidationObject extends ReactiveObject implements IValidatableViewModel {
  readonly ValidationContext = new ValidationContext();
  get HasErrors(): boolean { return this.ValidationContext.HasErrors; }
  GetErrors(propertyName?: string): readonly string[] { return this.ValidationContext.GetErrors(propertyName); }
  ValidationRule<T = unknown>(propertyName: string, predicate: ValidationPredicate<T>, message?: string): PropertyValidationRule<T> {
    return ValidationRule(this, propertyName, predicate, message);
  }
  override Dispose(): void { this.ValidationContext.Dispose(); super.Dispose(); }
}
