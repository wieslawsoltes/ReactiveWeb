import { Disposable, type IDisposable } from './disposables.js';

/** Explicit runtime tokens replace CLR Type values and erased JavaScript generics. */
export type BindingTypeToken<T = unknown> = string | symbol | Function;
export type ConversionResult<T = unknown> = { readonly Success: true; readonly Result: T; readonly success: true; readonly value: T } | { readonly Success: false; readonly Result?: undefined; readonly success: false; readonly value?: undefined; readonly Error?: unknown };
export const Conversion = Object.freeze({
  Success<T>(value: T): ConversionResult<T> { return { Success: true, Result: value, success: true, value }; },
  Failure(error?: unknown): ConversionResult<never> { return { Success: false, success: false, Error: error }; },
});
export interface IBindingTypeConverter<TFrom = unknown, TTo = unknown> {
  readonly FromType: BindingTypeToken<TFrom>;
  readonly ToType: BindingTypeToken<TTo>;
  GetAffinityForObjects(): number;
  TryConvertTyped(value: TFrom, conversionHint?: unknown): ConversionResult<TTo>;
}
export interface IBindingFallbackConverter {
  GetAffinityForObjects(fromType: BindingTypeToken, toType: BindingTypeToken): number;
  TryConvert(fromType: BindingTypeToken, value: unknown, toType: BindingTypeToken, conversionHint?: unknown): ConversionResult;
}
export interface ISetMethodBindingConverter {
  GetAffinityForObjects(fromType: BindingTypeToken, toType: BindingTypeToken): number;
  PerformSet(target: unknown, value: unknown, arguments_?: readonly unknown[]): unknown;
}

/** Subclass for custom conversion or use the constructor with an explicit callback. */
export class BindingTypeConverter<TFrom = unknown, TTo = unknown> implements IBindingTypeConverter<TFrom, TTo> {
  constructor(readonly FromType: BindingTypeToken<TFrom>, readonly ToType: BindingTypeToken<TTo>, private readonly convert?: (value: TFrom, hint?: unknown) => ConversionResult<TTo>, private readonly affinity = 10) {}
  GetAffinityForObjects(): number { return this.affinity; }
  TryConvertTyped(value: TFrom, hint?: unknown): ConversionResult<TTo> {
    if (!this.convert) return Conversion.Failure(new Error('Override TryConvertTyped or provide a conversion callback.'));
    try { return this.convert(value, hint); } catch (error) { return Conversion.Failure(error); }
  }
  TryConvert(value: TFrom, hint?: unknown): ConversionResult<TTo> { return this.TryConvertTyped(value, hint); }
}
class Registry<T> {
  protected readonly registrations: { value: T; identity: symbol }[] = [];
  Register(converter: T): IDisposable {
    if (!converter) throw new TypeError('A converter is required.');
    const entry = { value: converter, identity: Symbol() };
    this.registrations.push(entry);
    return Disposable.Create(() => { const index = this.registrations.indexOf(entry); if (index >= 0) this.registrations.splice(index, 1); });
  }
  GetAllConverters(): readonly T[] { return this.registrations.map(entry => entry.value); }
  Clear(): void { this.registrations.length = 0; }
  protected highest(score: (value: T) => number): T | undefined {
    let winner: T | undefined, best = 0;
    // Equal affinity prefers the latest registration, supporting scoped overrides.
    for (const { value } of this.registrations) { const affinity = score(value); if (Number.isFinite(affinity) && affinity > 0 && affinity >= best) { best = affinity; winner = value; } }
    return winner;
  }
}
export class TypedConverterRegistry extends Registry<IBindingTypeConverter<any, any>> {
  TryGetConverter(fromType: BindingTypeToken, toType: BindingTypeToken): IBindingTypeConverter<any, any> | undefined {
    return this.highest(converter => converter.FromType === fromType && converter.ToType === toType ? converter.GetAffinityForObjects() : 0);
  }
}
export class FallbackConverterRegistry extends Registry<IBindingFallbackConverter> {
  TryGetConverter(fromType: BindingTypeToken, toType: BindingTypeToken): IBindingFallbackConverter | undefined { return this.highest(converter => converter.GetAffinityForObjects(fromType, toType)); }
}
export class SetMethodConverterRegistry extends Registry<ISetMethodBindingConverter> {
  TryGetConverter(fromType: BindingTypeToken, toType: BindingTypeToken): ISetMethodBindingConverter | undefined { return this.highest(converter => converter.GetAffinityForObjects(fromType, toType)); }
}

export function GetBindingType(value: unknown): BindingTypeToken {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  switch (typeof value) {
    case 'string': return String;
    case 'number': return Number;
    case 'boolean': return Boolean;
    case 'bigint': return BigInt;
    case 'symbol': return Symbol;
    case 'function': return Function;
    default: return Object.getPrototypeOf(value)?.constructor ?? Object;
  }
}
export interface ResolvedBindingConverter { TryConvert(value: unknown, conversionHint?: unknown): ConversionResult }

/** Selects typed exact-token converters before fallback converters. Conversion failures are data. */
export class ConverterService {
  private static current?: ConverterService;
  static get Current(): ConverterService { return this.current ??= new ConverterService(); }
  static set Current(value: ConverterService) { this.current = value; }
  readonly TypedConverters = new TypedConverterRegistry();
  readonly FallbackConverters = new FallbackConverterRegistry();
  readonly SetMethodConverters = new SetMethodConverterRegistry();
  constructor(registerDefaults = true) { if (registerDefaults) RegisterDefaultConverters(this); }
  ResolveConverter(fromType: BindingTypeToken, toType: BindingTypeToken): ResolvedBindingConverter | undefined {
    const typed = this.TypedConverters.TryGetConverter(fromType, toType);
    if (typed) return { TryConvert: (value, hint) => typed.TryConvertTyped(value, hint) };
    const fallback = this.FallbackConverters.TryGetConverter(fromType, toType);
    if (fallback) return { TryConvert: (value, hint) => fallback.TryConvert(fromType, value, toType, hint) };
    return undefined;
  }
  ResolveSetMethodConverter(fromType: BindingTypeToken, toType: BindingTypeToken): ISetMethodBindingConverter | undefined { return this.SetMethodConverters.TryGetConverter(fromType, toType); }
  TryConvert<T = unknown>(value: unknown, toType: BindingTypeToken<T>, conversionHint?: unknown, fromType: BindingTypeToken = GetBindingType(value)): ConversionResult<T> {
    try { return (this.ResolveConverter(fromType, toType)?.TryConvert(value, conversionHint) ?? Conversion.Failure(new TypeError('No converter is registered for these types.'))) as ConversionResult<T>; }
    catch (error) { return Conversion.Failure(error); }
  }
  Convert<T = unknown>(value: unknown, toType: BindingTypeToken<T>, conversionHint?: unknown, fromType?: BindingTypeToken): T {
    const result = this.TryConvert<T>(value, toType, conversionHint, fromType);
    if (!result.Success) throw result.Error ?? new TypeError('The value cannot be converted.');
    return result.Result;
  }
}

export class EqualityTypeConverter implements IBindingFallbackConverter {
  GetAffinityForObjects(fromType: BindingTypeToken, toType: BindingTypeToken): number { return fromType === toType ? 1 : 0; }
  TryConvert(fromType: BindingTypeToken, value: unknown, toType: BindingTypeToken): ConversionResult {
    if (fromType !== toType) return Conversion.Failure();
    if (toType === Number && !Number.isFinite(value)) return Conversion.Failure(new TypeError('Expected a finite number.'));
    if (toType === Date && (!(value instanceof Date) || Number.isNaN(value.getTime()))) return Conversion.Failure(new TypeError('Expected a valid Date.'));
    return Conversion.Success(value);
  }
}
export class StringConverter implements IBindingFallbackConverter {
  GetAffinityForObjects(fromType: BindingTypeToken, toType: BindingTypeToken): number { return toType === String && [String, Number, Boolean, BigInt, 'null', 'undefined'].includes(fromType as any) ? 1 : 0; }
  TryConvert(_fromType: BindingTypeToken, value: unknown, toType: BindingTypeToken): ConversionResult<string> {
    if (toType !== String) return Conversion.Failure();
    return Conversion.Success(value == null ? '' : String(value));
  }
}
export class StringToNumberConverter extends BindingTypeConverter<string, number> {
  constructor() { super(String, Number, value => {
    if (typeof value !== 'string' || !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(value.trim())) return Conversion.Failure(new TypeError('Expected a decimal number.'));
    const number = Number(value);
    return Number.isFinite(number) ? Conversion.Success(number) : Conversion.Failure(new TypeError('Expected a finite number.'));
  }); }
}
export class NumberToStringConverter extends BindingTypeConverter<number, string> {
  constructor() { super(Number, String, value => Number.isFinite(value) ? Conversion.Success(String(value)) : Conversion.Failure(new TypeError('Expected a finite number.'))); }
}
export class StringToBooleanConverter extends BindingTypeConverter<string, boolean> {
  constructor() { super(String, Boolean, value => {
    if (typeof value !== 'string') return Conversion.Failure();
    const normalized = value.trim().toLowerCase();
    return normalized === 'true' || normalized === '1' ? Conversion.Success(true) : normalized === 'false' || normalized === '0' ? Conversion.Success(false) : Conversion.Failure(new TypeError('Expected true, false, 1 or 0.'));
  }); }
}
export class NumberToBooleanConverter extends BindingTypeConverter<number, boolean> {
  constructor() { super(Number, Boolean, value => value === 1 ? Conversion.Success(true) : value === 0 ? Conversion.Success(false) : Conversion.Failure(new TypeError('Expected 0 or 1.'))); }
}
export class StringToDateConverter extends BindingTypeConverter<string, Date> {
  constructor() { super(String, Date, value => {
    // Deterministic UTC parsing; local/culture date formats need an explicit custom converter.
    if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z)?$/.test(value)) return Conversion.Failure(new TypeError('Expected an ISO UTC date.'));
    const date = new Date(value);
    return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value.slice(0, 10) ? Conversion.Success(date) : Conversion.Failure(new TypeError('Invalid calendar date.'));
  }); }
}
export class DateToStringConverter extends BindingTypeConverter<Date, string> {
  constructor() { super(Date, String, value => value instanceof Date && Number.isFinite(value.getTime()) ? Conversion.Success(value.toISOString()) : Conversion.Failure(new TypeError('Expected a valid Date.'))); }
}
export function RegisterDefaultConverters(service: ConverterService): IDisposable {
  const registrations: IDisposable[] = [new StringToNumberConverter(), new NumberToStringConverter(), new StringToBooleanConverter(), new NumberToBooleanConverter(), new StringToDateConverter(), new DateToStringConverter()].map(converter => service.TypedConverters.Register(converter));
  registrations.push(service.FallbackConverters.Register(new EqualityTypeConverter()), service.FallbackConverters.Register(new StringConverter()));
  return Disposable.Create(() => { for (const registration of registrations) registration.Dispose(); });
}

export interface BindingHookContext { readonly ViewModel: object; readonly View: object; readonly SourceProperty: string; readonly TargetProperty: string; readonly Direction: 'OneWay' | 'TwoWay' }
export interface IPropertyBindingHook { ExecuteHook(context: BindingHookContext): boolean }
/** Providers may reject a binding before any subscriptions or event handlers are installed. */
export class PropertyBindingHookRegistry {
  static Current = new PropertyBindingHookRegistry();
  private readonly hooks = new Map<symbol, IPropertyBindingHook>();
  Register(hook: IPropertyBindingHook): IDisposable { const key = Symbol(); this.hooks.set(key, hook); return Disposable.Create(() => { this.hooks.delete(key); }); }
  ExecuteHooks(context: BindingHookContext): boolean { return [...this.hooks.values()].every(hook => hook.ExecuteHook(context)); }
}
export { TypedConverterRegistry as BindingTypeConverterRegistry, FallbackConverterRegistry as BindingFallbackConverterRegistry, SetMethodConverterRegistry as SetMethodBindingConverterRegistry };
export class RxConverters {
  static get Services(): ConverterService { return ConverterService.Current; }
  static set Services(value: ConverterService) { ConverterService.Current = value; }
}
