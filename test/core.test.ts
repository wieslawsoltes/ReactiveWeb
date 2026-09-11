import test from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorSubject, Observable, Subject, from, map, of, scan, throwError } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { CompositeDisposable, Disposable, DisposeWith, RefCountDisposable, SerialDisposable, SingleAssignmentDisposable } from '../dist/disposables.js';
import { ReactiveObject, defineReactiveProperties, ObservableForProperty, WhenAny, WhenAnyValue, WhenAnyObservable, getPropertyPath } from '../dist/reactive-object.js';
import { ObservableAsPropertyHelper, ToProperty } from '../dist/observable-property.js';
import { RxApp } from '../dist/rx-app.js';
import { ReactiveProperty, ToReactiveProperty } from '../dist/reactive-property.js';
import { ObservablePropertyProviders } from '../dist/providers.js';

test('disposables are idempotent, accept RxJS teardown and dispose late resources', () => {
  const calls: number[] = [];
  const group = new CompositeDisposable(Disposable.Create(() => calls.push(1)), () => calls.push(2));
  const subscription = new Subject().subscribe();
  DisposeWith(subscription, group);
  assert.equal(group.Count, 3);
  assert.equal(group.Remove(subscription), true);
  assert.equal(subscription.closed, true);
  group.Dispose();
  group.unsubscribe();
  group.Add(() => calls.push(3));
  assert.deepEqual(calls, [1, 2, 3]);
  assert.equal(group.Count, 0);
});

test('composite attempts every teardown even when resources throw', () => {
  const calls: number[] = [];
  const group = new CompositeDisposable(() => { throw Error('one'); }, () => calls.push(1), () => { throw Error('two'); });
  assert.throws(() => group.Dispose(), AggregateError);
  assert.deepEqual(calls, [1]);
  assert.equal(group.IsDisposed, true);
});

test('serial, single-assignment, and reference-counted lifetimes handle disposal races', () => {
  let count = 0;
  const serial = new SerialDisposable();
  serial.Disposable = () => count++;
  serial.Disposable = () => count++;
  assert.equal(count, 1);
  serial.Dispose();
  serial.Disposable = () => count++;
  assert.equal(count, 3);
  const single = new SingleAssignmentDisposable();
  single.Dispose();
  single.Disposable = () => count++;
  assert.equal(count, 4);
  assert.throws(() => { single.Disposable = Disposable.Empty; }, /already/);
  const counted = new RefCountDisposable(() => count++);
  const first = counted.GetDisposable();
  const second = counted.GetDisposable();
  counted.Dispose(); first.Dispose(); first.Dispose();
  assert.equal(count, 4);
  second.Dispose();
  assert.equal(count, 5);
  assert.equal(counted.IsDisposed, true);
  counted.GetDisposable().Dispose();
  assert.equal(count, 5);
});

test('ReactiveObject accessor backing store and Changing/Changed ordering are consistent', () => {
  const vm = defineReactiveProperties(new ReactiveObject(), { Name: 'Ada', Count: 1 });
  const observations: unknown[] = [];
  vm.Changing.subscribe(e => observations.push(['before', e.PropertyName, vm.Name, e.OldValue, e.Value]));
  vm.Changed.subscribe(e => observations.push(['after', e.PropertyName, vm.Name, e.OldValue, e.Value]));
  vm.Name = 'Grace';
  vm.Name = 'Grace';
  assert.deepEqual(observations, [['before', 'Name', 'Ada', 'Ada', 'Grace'], ['after', 'Name', 'Grace', 'Ada', 'Grace']]);
  assert.equal(vm.RaiseAndSetIfChanged('Count', 3), 3);
  assert.equal(vm.Count, 3);
  assert.equal(vm.GetValue('missing', 10), 10);
  assert.throws(() => defineReactiveProperties(vm, { Changed: 1 }), /conflicts/);
});

test('suppression nests; delay coalesces last-change order and retains reverted notifications', () => {
  const vm = defineReactiveProperties(new ReactiveObject(), { A: 0, B: 0 });
  const changes: unknown[] = [];
  vm.Changed.subscribe(e => changes.push([e.PropertyName, e.OldValue, e.Value]));
  const outer = vm.SuppressChangeNotifications();
  const inner = vm.SuppressChangeNotifications();
  vm.A = 1; outer.Dispose();
  assert.equal(vm.AreChangeNotificationsEnabled, false);
  inner.Dispose();
  assert.equal(vm.AreChangeNotificationsEnabled, true);
  const delay = vm.DelayChangeNotifications();
  const delay2 = vm.DelayChangeNotifications();
  vm.A = 2; vm.B = 4; vm.A = 1;
  delay.Dispose();
  assert.deepEqual(changes, []);
  delay2.Dispose();
  assert.deepEqual(changes, [['B', 0, 4], ['A', 1, 1]]);
});

test('nested observation follows replacement/null and detaches every old branch', () => {
  const old = defineReactiveProperties(new ReactiveObject(), { Name: 'old' });
  const next = defineReactiveProperties(new ReactiveObject(), { Name: 'next' });
  const vm = defineReactiveProperties(new ReactiveObject(), { Person: old as typeof old | null });
  const values: unknown[] = [];
  const subscription = WhenAnyValue(vm, 'Person.Name').subscribe(value => values.push(value));
  old.Name = 'old2'; vm.Person = next; old.Name = 'ignored'; vm.Person = null; next.Name = 'ignored2'; vm.Person = next;
  subscription.unsubscribe(); next.Name = 'also ignored';
  assert.deepEqual(values, ['old', 'old2', 'next', undefined, 'ignored2']);
});

test('property paths support selectors, brackets, and explicit event/value projectors', () => {
  assert.deepEqual(getPropertyPath((vm: any) => vm.User.Name), ['User', 'Name']);
  assert.deepEqual(getPropertyPath('Items[0]["a.b"]'), ['Items', '0', 'a.b']);
  assert.throws(() => getPropertyPath('broken..path'), /Invalid/);
  const vm = defineReactiveProperties(new ReactiveObject(), { First: 'Ada', Last: 'Lovelace' });
  const values: unknown[] = [];
  const events: unknown[] = [];
  const a = WhenAnyValue(vm, 'First', 'Last', (first: string, last: string) => `${first} ${last}`).subscribe(x => values.push(x));
  const b = WhenAny(vm, (x: typeof vm) => x.First, (change: any) => change.Value).subscribe(x => events.push(x));
  vm.First = 'Augusta';
  assert.deepEqual(values, ['Ada Lovelace', 'Augusta Lovelace']);
  assert.deepEqual(events, ['Ada', 'Augusta']);
  a.unsubscribe(); b.unsubscribe();
});

test('ObservableForProperty beforeChange rebinds when intermediate objects change', () => {
  const a = defineReactiveProperties(new ReactiveObject(), { Name: 'a' });
  const b = defineReactiveProperties(new ReactiveObject(), { Name: 'b' });
  const vm = defineReactiveProperties(new ReactiveObject(), { Person: a });
  const values: unknown[] = [];
  const subscription = ObservableForProperty(vm, 'Person.Name', { beforeChange: true, distinct: false }).subscribe(x => values.push(x.Value));
  vm.Person = b; b.Name = 'c'; a.Name = 'ignored'; subscription.unsubscribe();
  assert.deepEqual(values, ['a', 'a', 'b']);
});

test('WhenAnyObservable switches inner streams, merges multiple sources, projects latest values', () => {
  const a = new Subject<number>(); const b = new Subject<number>(); const c = new Subject<number>();
  const vm = defineReactiveProperties(new ReactiveObject(), { A: a, B: b });
  const merged: unknown[] = []; const projected: unknown[] = [];
  const s = WhenAnyObservable(vm, 'A', 'B').subscribe(x => merged.push(x));
  const p = WhenAnyObservable(vm, 'A', 'B', (x: number, y: number) => x + y).subscribe(x => projected.push(x));
  a.next(1); b.next(2); vm.A = c; a.next(99); c.next(3);
  assert.deepEqual(merged, [1, 2, 3]);
  assert.deepEqual(projected, [3, 5]);
  s.unsubscribe(); p.unsubscribe();
  assert.equal(a.observed || b.observed || c.observed, false);
});

test('ToProperty installs a read-only cached value with notifications and subscription cleanup', () => {
  const source = new Subject<number>();
  const owner = new ReactiveObject() as ReactiveObject & { Total: number };
  const records: unknown[] = [];
  const helper = ToProperty(source, owner, 'Total', { initialValue: 0 });
  owner.Changing.subscribe(e => records.push(['before', owner.Total, e.Value]));
  owner.Changed.subscribe(e => records.push(['after', owner.Total, e.Value]));
  source.next(4); source.next(4); source.next(5);
  assert.equal(owner.Total, 5);
  assert.deepEqual(records, [['before', 0, 4], ['after', 4, 4], ['before', 4, 5], ['after', 5, 5]]);
  assert.throws(() => { owner.Total = 9; }, TypeError);
  helper.Dispose(); source.next(6);
  assert.equal(owner.Total, 5);
  assert.equal(source.observed, false);
});

test('observable property defers subscription until Value; disposal before read prevents subscription', () => {
  let subscriptions = 0;
  const source = new Observable<number>(observer => { subscriptions++; observer.next(7); return () => subscriptions--; });
  const helper = new ObservableAsPropertyHelper(source, { initialValue: 2, deferSubscription: true });
  assert.equal(helper.IsSubscribed, false);
  assert.equal(helper.Value, 7);
  assert.equal(subscriptions, 1);
  helper.Dispose();
  assert.equal(subscriptions, 0);
  const disposed = new ObservableAsPropertyHelper(source, { initialValue: 2, deferSubscription: true });
  disposed.Dispose();
  assert.equal(disposed.Value, 2);
  assert.equal(subscriptions, 0);
});

test('observable property schedules delivery and routes observed and unobserved errors', () => {
  const scheduler = new TestScheduler(() => {});
  const helper = new ObservableAsPropertyHelper(of(7), { initialValue: 0, scheduler });
  assert.equal(helper.Value, 0);
  scheduler.flush();
  assert.equal(helper.Value, 7);
  const failure = new Error('failure');
  const deferred = new ObservableAsPropertyHelper(throwError(() => failure), { deferSubscription: true });
  const errors: unknown[] = [];
  deferred.ThrownExceptions.subscribe(e => errors.push(e));
  void deferred.Value;
  assert.deepEqual(errors, [failure]);
  const previousHandler = RxApp.DefaultExceptionHandler;
  try {
    RxApp.DefaultExceptionHandler = e => errors.push(e);
    new ObservableAsPropertyHelper(throwError(() => failure));
    assert.equal(errors.length, 2);
  } finally { RxApp.DefaultExceptionHandler = previousHandler; }
});

test('ReactiveProperty is an RxJS source and observer with equality, refresh, and source lifetime', () => {
  const property = new ReactiveProperty(1);
  const values: number[] = [];
  const subscription = from(property).pipe(map(x => x * 2)).subscribe(x => values.push(x));
  property.Value = 2; property.Value = 2; property.Refresh();
  assert.deepEqual(values, [2, 4, 4]);
  const source = new Subject<number>();
  const derived = ToReactiveProperty(source, { initialValue: 10 });
  assert.equal(derived.Value, 10);
  source.next(12);
  assert.equal(derived.Value, 12);
  derived.Dispose();
  assert.equal(source.observed, false);
  subscription.unsubscribe(); property.Dispose();
});

test('ReactiveProperty subscription options preserve late-subscriber semantics', () => {
  const property = ReactiveProperty.Create(1, { allowDuplicateValues: true, skipCurrentValueOnSubscribe: true });
  const values: number[] = [];
  property.subscribe(value => values.push(value));
  property.Value = 1; property.Value = 2;
  assert.deepEqual(values, [1, 2]);
  property.Dispose();
});

test('ReactiveProperty validates sync values, removes rules, and hides initial errors on request', () => {
  const property = new ReactiveProperty('', { ignoreInitialError: true });
  property.AddValidationError(value => value ? null : 'Required');
  assert.equal(property.HasErrors, false);
  property.CheckValidation();
  assert.equal(property.HasErrors, true);
  assert.deepEqual(property.GetErrors('Value'), ['Required']);
  assert.deepEqual(property.GetErrors('Other'), []);
  property.Value = 'yes';
  assert.equal(property.HasErrors, false);
  const rule = property.AddValidator(value => value.length < 4 ? 'Too short' : null);
  assert.deepEqual(property.Errors, ['Too short']);
  rule.Dispose();
  assert.equal(property.HasErrors, false);
  property.Dispose();
});

test('ReactiveProperty cancels stale asynchronous validation and releases observable validators', async () => {
  const property = new ReactiveProperty('first');
  const pending = new Map<string, (error: string | null) => void>();
  property.AddValidationError(value => new Promise(resolve => pending.set(value, resolve)));
  assert.equal(property.IsValidating, true);
  property.Value = 'second';
  pending.get('second')!(null);
  await Promise.resolve();
  assert.equal(property.IsValidating, false);
  pending.get('first')!('stale error');
  await Promise.resolve();
  assert.deepEqual(property.Errors, []);
  const errorStream = new Subject<string | null>();
  property.AddValidationError(() => errorStream);
  assert.equal(errorStream.observed, true);
  property.Dispose();
  assert.equal(errorStream.observed, false);
});

test('ReactiveProperty stream validators retain operator history across value changes', () => {
  const property = new ReactiveProperty(1);
  property.AddValidationErrorObservable(values => values.pipe(scan((sum, value) => sum + value, 0), map(sum => sum > 5 ? 'Total exceeded' : null)));
  property.Value = 2;
  assert.equal(property.HasErrors, false);
  property.Value = 3;
  assert.deepEqual(property.Errors, ['Total exceeded']);
  property.Dispose();
});

test('explicit validation refreshes stream validators without emitting duplicate property values', () => {
  const property = new ReactiveProperty('value');
  let invalid = false;
  property.AddValidationErrorObservable(values => values.pipe(map(() => invalid ? 'External state invalid' : null)));
  const emitted: string[] = [];
  property.subscribe(value => emitted.push(value));
  invalid = true;
  property.CheckValidation();
  assert.deepEqual(property.Errors, ['External state invalid']);
  assert.deepEqual(emitted, ['value']);
  property.Dispose();
});

test('ReactiveProperty disposal permits reentrant teardown and completes despite teardown errors', () => {
  let property: ReactiveProperty<number>;
  const source = new Observable<number>(observer => { observer.next(1); return () => property.Dispose(); });
  property = new ReactiveProperty(source);
  property.Dispose();
  assert.equal(property.IsDisposed, true);
  const failure = new ReactiveProperty(new Observable<number>(() => () => { throw Error('teardown'); }));
  let completed = false;
  failure.ObserveErrorChanged.subscribe({ complete: () => { completed = true; } });
  assert.throws(() => failure.Dispose(), /teardown/);
  assert.equal(failure.IsDisposed, true);
  assert.equal(completed, true);
});

test('custom property providers observe plain-object paths and detach after replacement', () => {
  class Model { constructor(public Name: string) {} changes = new Subject<void>(); }
  const registration = ObservablePropertyProviders.Current.Register({
    GetAffinityForObject(type: Function) { return type === Model ? 10 : 0; },
    GetNotificationForProperty(sender: object) { return (sender as Model).changes; }
  });
  try {
    const first = new Model('first'); const second = new Model('second');
    const root = defineReactiveProperties(new ReactiveObject(), { Model: first });
    const values: unknown[] = [];
    const subscription = WhenAnyValue(root, 'Model.Name').subscribe(value => values.push(value));
    first.Name = 'updated'; first.changes.next(); root.Model = second;
    first.Name = 'ignored'; first.changes.next(); second.Name = 'new'; second.changes.next();
    subscription.unsubscribe();
    assert.deepEqual(values, ['first', 'updated', 'second', 'new']);
    assert.equal(first.changes.observed || second.changes.observed, false);
  } finally { registration.Dispose(); }
});
