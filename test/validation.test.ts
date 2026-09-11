import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Observable, Subject } from 'rxjs';
import { ReactiveValidationObject, PropertyValidationRule, ValidationRule } from '../dist/validation.js';

test('validation aggregates property errors and removes disposed rules', () => {
  const vm = new ReactiveValidationObject({ Name: '', Age: 2 }) as ReactiveValidationObject & { Name: string; Age: number };
  const name = ValidationRule<string>(vm, 'Name', value => value.length >= 2, 'Name is required.');
  const age = ValidationRule<number>(vm, 'Age', value => value >= 18, 'Must be an adult.');
  assert.equal(vm.ValidationContext.IsValidValue, false);
  assert.deepEqual(vm.GetErrors('Name'), ['Name is required.']);
  vm.Name = 'Ada';
  assert.deepEqual(vm.GetErrors('Name'), []);
  age.Dispose();
  assert.equal(vm.ValidationContext.IsValidValue, true);
  assert.equal(vm.ValidationContext.Validations.length, 1);
  name.Dispose(); vm.Dispose();
});

test('async validation cancels obsolete promises and keeps pending invalid', async () => {
  const vm = new ReactiveValidationObject({ Name: 'first' }) as ReactiveValidationObject & { Name: string };
  const resolvers = new Map<string, (value: boolean) => void>();
  ValidationRule(vm, 'Name', value => new Promise<boolean>(resolve => resolvers.set(value as string, resolve)), 'Already used.');
  assert.equal(vm.ValidationContext.IsPendingValue, true);
  assert.equal(vm.ValidationContext.IsValidValue, false);
  vm.Name = 'second';
  resolvers.get('second')!(true);
  await Promise.resolve();
  assert.equal(vm.ValidationContext.IsValidValue, true);
  resolvers.get('first')!(false);
  await Promise.resolve();
  assert.equal(vm.ValidationContext.IsValidValue, true);
  vm.Dispose();
});

test('validation unsubscribes asynchronous work on change and context disposal', () => {
  const vm = new ReactiveValidationObject({ Code: 'a' }) as ReactiveValidationObject & { Code: string };
  let active = 0;
  ValidationRule(vm, 'Code', () => new Observable<boolean>(subscriber => { active++; subscriber.next(true); return () => { active--; }; }));
  assert.equal(active, 1);
  vm.Code = 'b';
  assert.equal(active, 1);
  vm.ValidationContext.Dispose();
  assert.equal(active, 0);
  vm.Dispose();
});

test('a throwing validator becomes invalid and recovers on the next value', () => {
  const vm = new ReactiveValidationObject({ Value: 0 }) as ReactiveValidationObject & { Value: number };
  ValidationRule<number>(vm, 'Value', value => { if (!value) throw new Error('Bad input'); return true; }, 'Try again.');
  assert.deepEqual(vm.GetErrors(), ['Try again.']);
  vm.Value = 1;
  assert.equal(vm.ValidationContext.IsValidValue, true);
  vm.Dispose();
});

test('Observable validation results can carry multiple messages', () => {
  const vm = new ReactiveValidationObject();
  const results = new Subject<string[]>();
  const values = new Subject<number>();
  const rule = new PropertyValidationRule(vm.ValidationContext, 'Code', values, () => results);
  values.next(1);
  results.next(['Too short', 'Use digits']);
  assert.deepEqual(vm.GetErrors('Code'), ['Too short', 'Use digits']);
  results.next([]);
  assert.equal(vm.ValidationContext.IsValidValue, true);
  rule.Dispose(); vm.Dispose();
});
