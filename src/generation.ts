/** Runtime alternatives to .NET source-generated reactive members. */
import { type Observable, type ObservableInput, type SchedulerLike } from 'rxjs';
import { ReactiveObject } from './reactive-object.js';
import { ToProperty } from './observable-property.js';
import { ReactiveCommand } from './command.js';

const reservedReactiveNames = (() => {
  const probe = new ReactiveObject();
  const names = new Set(Object.getOwnPropertyNames(probe));
  probe.Dispose();
  return names;
})();

export interface ReactivePropertyOptions<T = unknown> {
  /** Rejects a write before any property notification or state mutation. */
  validate?: (value: T) => boolean | string;
  /** Replaces Object.is when deciding whether a write changes the property. */
  equals?: (previous: T, next: T) => boolean;
  /** Names of synchronous computed getters affected by this property. */
  dependents?: readonly string[];
}

/** An invalid reactive property assignment; the previous value remains intact. */
export class ReactivePropertyValidationError extends TypeError {
  constructor(public readonly PropertyName: string, public readonly AttemptedValue: unknown, message?: string) {
    super(message || `Invalid value for reactive property '${PropertyName}'.`);
    this.name = 'ReactivePropertyValidationError';
  }
}

function validate<T>(name: string, value: T, options: ReactivePropertyOptions<T>): void {
  const result = options.validate?.(value);
  if (result === false || typeof result === 'string') {
    throw new ReactivePropertyValidationError(name, value, typeof result === 'string' ? result : undefined);
  }
}

function setReactive<T>(owner: ReactiveObject, name: string, previous: T, value: T, options: ReactivePropertyOptions<T>): T {
  validate(name, value, options);
  if ((options.equals ?? Object.is)(previous, value)) return previous;
  const dependents = [...new Set(options.dependents ?? [])].filter(dependent => dependent !== name);
  const previousValues = dependents.map(dependent => (owner as unknown as Record<string, unknown>)[dependent]);
  for (let index = 0; index < dependents.length; index++) owner.RaisePropertyChanging(dependents[index], previousValues[index]);
  const next = owner.RaiseAndSetIfChanged(name, value);
  for (let index = 0; index < dependents.length; index++) owner.RaisePropertyChanged(dependents[index], previousValues[index], (owner as unknown as Record<string, unknown>)[dependents[index]]);
  return next;
}

export type ReactiveAccessorDecorator = <TThis extends ReactiveObject, TValue>(
  target: ClassAccessorDecoratorTarget<TThis, TValue>,
  context: ClassAccessorDecoratorContext<TThis, TValue>,
) => ClassAccessorDecoratorResult<TThis, TValue>;

/**
 * Standard TypeScript 5+ auto-accessor decorator. Use `@Reactive accessor Name = ''`.
 * Does not require legacy experimentalDecorators, Proxy, or reflection metadata.
 */
export function Reactive<TThis extends ReactiveObject, TValue>(
  target: ClassAccessorDecoratorTarget<TThis, TValue>,
  context: ClassAccessorDecoratorContext<TThis, TValue>,
): ClassAccessorDecoratorResult<TThis, TValue>;
export function Reactive(): ReactiveAccessorDecorator;
export function Reactive<TValue>(options: ReactivePropertyOptions<TValue>): <TThis extends ReactiveObject>(
  target: ClassAccessorDecoratorTarget<TThis, TValue>,
  context: ClassAccessorDecoratorContext<TThis, TValue>,
) => ClassAccessorDecoratorResult<TThis, TValue>;
export function Reactive(
  targetOrOptions: ClassAccessorDecoratorTarget<ReactiveObject, unknown> | ReactivePropertyOptions<unknown> = {},
  context?: ClassAccessorDecoratorContext<ReactiveObject, unknown>,
): ClassAccessorDecoratorResult<ReactiveObject, unknown> | ReactiveAccessorDecorator {
  if (!context) {
    const options = targetOrOptions as ReactivePropertyOptions<unknown>;
    return ((target: ClassAccessorDecoratorTarget<ReactiveObject, unknown>, currentContext: ClassAccessorDecoratorContext<ReactiveObject, unknown>) =>
      decorateReactive(target, currentContext, options)) as ReactiveAccessorDecorator;
  }
  return decorateReactive(targetOrOptions as ClassAccessorDecoratorTarget<ReactiveObject, unknown>, context, {});
}

function decorateReactive<TThis extends ReactiveObject, T>(
  target: ClassAccessorDecoratorTarget<TThis, T>,
  context: ClassAccessorDecoratorContext<TThis, T>,
  options: ReactivePropertyOptions<T>,
): ClassAccessorDecoratorResult<TThis, T> {
  if (context.kind !== 'accessor' || context.static || context.private || typeof context.name !== 'string') {
    throw new TypeError('@Reactive requires a public, non-static, string-named auto-accessor on ReactiveObject.');
  }
  const name = context.name;
  if (reservedReactiveNames.has(name) || name in ReactiveObject.prototype || name === '__proto__') {
    throw new TypeError(`Reserved reactive property name '${name}'.`);
  }
  return {
    init(this: TThis, value: T): T {
      validate(name, value, options);
      const suppressed = this.SuppressChangeNotifications();
      try { this.SetValue(name, value); } finally { suppressed.Dispose(); }
      return value;
    },
    get(this: TThis): T { return this.GetValue<T>(name); },
    set(this: TThis, value: T): void {
      setReactive(this, name, this.GetValue<T>(name), value, options);
    },
  };
}

export interface ReactivePropertyDefinition<T = unknown> extends ReactivePropertyOptions<T> {
  initial: T;
  /** Per-instance initialization for values that cannot be structured-cloned. */
  factory?: () => T;
}

/** Preserves the property type while adding validation and dependent notifications. */
export function reactiveProperty<T>(initial: T, options: Omit<ReactivePropertyDefinition<T>, 'initial'> = {}): ReactivePropertyDefinition<T> {
  return { initial, ...options };
}

export type PropertyValues<P extends Record<string, ReactivePropertyDefinition<any>>> = {
  [K in keyof P]: P[K] extends ReactivePropertyDefinition<infer T> ? T : never;
};

export interface ComputedPropertyDefinition<T = unknown, TViewModel = any> {
  source: (viewModel: TViewModel) => Observable<T>;
  initialValue?: T;
  scheduler?: SchedulerLike;
  deferSubscription?: boolean;
  comparer?: (previous: T, next: T) => boolean;
}

export type GeneratedCommandDefinition<TInput = any, TOutput = any, TViewModel = any> = {
  canExecute?: (viewModel: TViewModel) => Observable<boolean>;
  outputScheduler?: SchedulerLike;
} & (
  | { kind?: 'sync'; execute: (viewModel: TViewModel, input: TInput) => TOutput }
  | { kind: 'task'; execute: (viewModel: TViewModel, input: TInput, signal: AbortSignal) => PromiseLike<TOutput> }
  | { kind: 'observable'; execute: (viewModel: TViewModel, input: TInput, signal: AbortSignal) => ObservableInput<TOutput> }
);

type ComputedValues<C extends Record<string, ComputedPropertyDefinition<any, any>>> = {
  readonly [K in keyof C]: C[K] extends ComputedPropertyDefinition<infer T, any>
    ? C[K] extends { initialValue: unknown } ? T : T | undefined
    : never;
};
type CommandValues<C extends Record<string, GeneratedCommandDefinition>> = {
  readonly [K in keyof C]: C[K] extends GeneratedCommandDefinition<infer TInput, infer TOutput> ? ReactiveCommand<TInput, TOutput> : never;
};
export interface GeneratedViewModel extends ReactiveObject {
  /** Releases generated commands and observable-as-property subscriptions. */
  Dispose(): void;
}

export interface ViewModelSchema<
  P extends Record<string, ReactivePropertyDefinition<any>>,
  C extends Record<string, ComputedPropertyDefinition<any, ReactiveObject & PropertyValues<P>>>,
  M extends Record<string, GeneratedCommandDefinition<any, any, ReactiveObject & PropertyValues<P>>>,
> {
  name?: string;
  properties: P;
  computed?: C & Record<string, ComputedPropertyDefinition<any, ReactiveObject & PropertyValues<P>>>;
  commands?: M & Record<string, GeneratedCommandDefinition<any, any, ReactiveObject & PropertyValues<P>>>;
}

/**
 * Creates one class with regular prototype accessors. Every instance owns its
 * property values, computed subscriptions, and commands. No Proxy is involved.
 */
export function defineViewModel<
  P extends Record<string, ReactivePropertyDefinition<any>>,
  C extends Record<string, ComputedPropertyDefinition<any, ReactiveObject & PropertyValues<P>>> = {},
  M extends Record<string, GeneratedCommandDefinition<any, any, ReactiveObject & PropertyValues<P>>> = {},
>(schema: ViewModelSchema<P, C, M>): new (initial?: Partial<PropertyValues<P>>) => GeneratedViewModel & PropertyValues<P> & ComputedValues<C> & CommandValues<M> {
  const properties = Object.entries(schema.properties);
  const computed = Object.entries(schema.computed ?? {}) as [string, ComputedPropertyDefinition][];
  const commands = Object.entries(schema.commands ?? {}) as [string, GeneratedCommandDefinition][];
  const names = [...properties, ...computed, ...commands].map(([name]) => name);
  if (names.length !== new Set(names).size) throw new TypeError('Reactive, computed, and command member names must be unique.');
  for (const name of names) {
    if (!name || reservedReactiveNames.has(name) || name in ReactiveObject.prototype || ['__proto__', 'prototype', 'constructor', 'Dispose', 'Changed', 'Changing', 'PropertyChanged', 'PropertyChanging', 'ThrownExceptions', 'changed', 'changing', 'thrownExceptions', '__generatedResources', '__generatedDisposed'].includes(name)) {
      throw new TypeError(`Reserved or invalid generated member name '${name}'.`);
    }
  }
  class SchemaViewModel extends ReactiveObject {
    #resources: { Dispose(): void }[] = [];
    #disposed = false;
    constructor(initial: Partial<PropertyValues<P>> = {}) {
      super();
      for (const name of Object.keys(initial)) {
        if (!Object.hasOwn(schema.properties, name)) throw new TypeError(`Unknown reactive property '${name}'.`);
      }
      try {
        for (const [name, definition] of properties) {
          const value = Object.hasOwn(initial, name) ? (initial as Record<string, unknown>)[name]
            : definition.factory ? definition.factory() : cloneInitial(definition.initial);
          validate(name, value, definition);
          this.RaiseAndSetIfChanged(name, value);
        }
        for (const [name, definition] of computed) {
          const helper = ToProperty(definition.source(this), this, name, {
            initialValue: definition.initialValue,
            ...(definition.scheduler ? { scheduler: definition.scheduler } : {}),
            ...(definition.deferSubscription !== undefined ? { deferSubscription: definition.deferSubscription } : {}),
            ...(definition.comparer ? { comparer: definition.comparer } : {}),
          });
          this.#resources.push(helper);
        }
        for (const [name, definition] of commands) {
          const canExecute = definition.canExecute?.(this);
          const command = definition.kind === 'task'
            ? ReactiveCommand.CreateFromTask((input: any, signal: AbortSignal) => definition.execute(this, input, signal), canExecute, definition.outputScheduler)
            : definition.kind === 'observable'
              ? ReactiveCommand.CreateFromObservable((input: any, signal: AbortSignal) => definition.execute(this, input, signal), canExecute, definition.outputScheduler)
              : ReactiveCommand.Create((input: any) => definition.execute(this, input), canExecute, definition.outputScheduler);
          Object.defineProperty(this, name, { value: command, enumerable: true });
          this.#resources.push(command);
        }
      } catch (error) {
        this.Dispose();
        throw error;
      }
    }
    override Dispose(): void {
      if (this.#disposed) return;
      this.#disposed = true;
      const errors: unknown[] = [];
      for (const resource of this.#resources.splice(0).reverse()) {
        try { resource.Dispose(); } catch (error) { errors.push(error); }
      }
      super.Dispose();
      if (errors.length) throw new AggregateError(errors, 'Generated view model disposal failed.');
    }
  }
  if (schema.name) Object.defineProperty(SchemaViewModel, 'name', { value: schema.name });
  for (const [name, definition] of properties) {
    Object.defineProperty(SchemaViewModel.prototype, name, {
      enumerable: true,
      get(this: SchemaViewModel) { return this.GetValue(name); },
      set(this: SchemaViewModel, value: unknown) { setReactive(this, name, this.GetValue(name), value, definition); },
    });
  }
  return SchemaViewModel as unknown as new (initial?: Partial<PropertyValues<P>>) => GeneratedViewModel & PropertyValues<P> & ComputedValues<C> & CommandValues<M>;
}

function cloneInitial<T>(value: T): T {
  if (value === null || typeof value !== 'object') return value;
  try { return structuredClone(value); }
  catch { throw new TypeError('Reactive property initial values must be cloneable; provide factory for class instances or non-cloneable values.'); }
}

/** Familiar .NET spelling of the modern runtime class factory. */
export const DefineViewModel = defineViewModel;
