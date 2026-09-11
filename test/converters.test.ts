import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ConverterService, BindingTypeConverter, Conversion, PropertyBindingHookRegistry } from '../dist/converters.js';

test('converter service handles finite decimal numbers and safe booleans', () => {
  const service = new ConverterService();
  assert.equal(service.Convert(' 1.25e2 ', Number), 125);
  assert.equal(service.Convert(12, String), '12');
  assert.equal(service.Convert('false', Boolean), false);
  assert.equal(service.Convert('TRUE', Boolean), true);
  assert.equal(service.Convert(0, Boolean), false);
  assert.equal(service.TryConvert('', Number).Success, false);
  assert.equal(service.TryConvert('0xff', Number).Success, false);
  assert.equal(service.TryConvert('Infinity', Number).Success, false);
  assert.equal(service.TryConvert(NaN, String).Success, false);
  assert.equal(service.TryConvert('yes', Boolean).Success, false);
  assert.equal(service.TryConvert(2, Boolean).Success, false);
});

test('date conversion rejects rollover and culture-dependent values', () => {
  const service = new ConverterService();
  const date = service.Convert<Date>('2024-02-29', Date);
  assert.equal(service.Convert(date, String), '2024-02-29T00:00:00.000Z');
  assert.equal(service.TryConvert('2025-02-29', Date).Success, false);
  assert.equal(service.TryConvert('09/11/2026', Date).Success, false);
  assert.equal(service.TryConvert(new Date(NaN), String).Success, false);
});

test('converter affinity, typed precedence, fallback and scoped overrides are deterministic', () => {
  const service = new ConverterService(false);
  const token = Symbol('document');
  service.FallbackConverters.Register({ GetAffinityForObjects: () => 100, TryConvert: () => Conversion.Success('fallback') });
  const original = service.TypedConverters.Register(new BindingTypeConverter(token, String, () => Conversion.Success('original'), 5));
  const override = service.TypedConverters.Register(new BindingTypeConverter(token, String, () => Conversion.Success('override'), 5));
  assert.equal(service.Convert({}, String, undefined, token), 'override');
  override.Dispose(); assert.equal(service.Convert({}, String, undefined, token), 'original');
  original.Dispose(); assert.equal(service.Convert({}, String, undefined, token), 'fallback');
});

test('custom converter exceptions are reported without throwing from TryConvert', () => {
  const service = new ConverterService();
  service.TypedConverters.Register(new BindingTypeConverter(String, Number, () => { throw new Error('Parsing failed'); }, 100));
  const result = service.TryConvert('12', Number);
  assert.equal(result.Success, false);
  assert.throws(() => service.Convert('12', Number), /Parsing failed/);
});

test('set-method converters and binding providers can be registered and removed', () => {
  const service = new ConverterService(false);
  const collection = Symbol('collection');
  const registration = service.SetMethodConverters.Register({ GetAffinityForObjects: (from, to) => from === Array && to === collection ? 10 : 0, PerformSet: (target, value) => { (target as unknown[]).splice(0, Infinity, ...(value as unknown[])); return target; } });
  const target = [1, 2]; service.ResolveSetMethodConverter(Array, collection)!.PerformSet(target, [3]); assert.deepEqual(target, [3]);
  registration.Dispose(); assert.equal(service.ResolveSetMethodConverter(Array, collection), undefined);
  const hooks = new PropertyBindingHookRegistry();
  const context = { ViewModel: {}, View: {}, SourceProperty: 'Password', TargetProperty: 'textContent', Direction: 'OneWay' as const };
  const hook = hooks.Register({ ExecuteHook: value => value.SourceProperty !== 'Password' });
  assert.equal(hooks.ExecuteHooks(context), false); hook.Dispose(); assert.equal(hooks.ExecuteHooks(context), true);
});
