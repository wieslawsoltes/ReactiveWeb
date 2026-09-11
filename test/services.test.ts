import test from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorSubject, Observable, Subject, firstValueFrom, of, throwError } from 'rxjs';
import { ViewModelActivator, WhenActivated } from '../dist/activation.js';
import { ObservableCollection, BindableDerivedList, OrderedComparer, ActOnEveryObject } from '../dist/collections.js';
import { Disposable } from '../dist/disposables.js';
import { ServiceLocator, ViewLocator, MessageBus } from '../dist/services.js';
import { RoutingState, WhenNavigatedTo, WhenNavigatedFrom } from '../dist/routing.js';
import { SuspensionHost, InMemorySuspensionDriver, LocalStorageSuspensionDriver, AutoPersist, AutoPersistCollection } from '../dist/persistence.js';

const delay = (ms = 0) => new Promise(resolve => setTimeout(resolve, ms));

test('activation shares a resource scope and recreates it only after final release', () => {
  const activator = new ViewModelActivator(); let starts = 0, stops = 0;
  WhenActivated(activator, bag => { starts++; bag.Add(() => stops++); });
  const first = activator.Activate(), second = activator.Activate();
  assert.equal(starts, 1); assert.equal(activator.ReferenceCount, 2);
  first.Dispose(); first.Dispose(); assert.equal(stops, 0);
  second.Dispose(); assert.equal(stops, 1); assert.equal(activator.IsActiveValue, false);
  const third = activator.Activate(); assert.equal(starts, 2);
  activator.Deactivate(true); const fourth = activator.Activate(); third.Dispose();
  assert.equal(activator.ReferenceCount, 1); fourth.Dispose(); assert.equal(stops, 3);
  activator.Dispose(); assert.throws(() => activator.Activate(), /disposed/);
});

test('activation handles duplicate callbacks and cleans up failed registrations', () => {
  const activator = new ViewModelActivator(); let starts = 0, stops = 0;
  const block = (bag: any) => { starts++; bag.Add(() => stops++); };
  const firstRegistration = WhenActivated(activator, block); WhenActivated(activator, block);
  const lease = activator.Activate(); assert.equal(starts, 2);
  firstRegistration.Dispose();
  assert.throws(() => WhenActivated(activator, bag => { bag.Add(() => stops++); throw Error('failure'); }), /failure/);
  assert.equal(stops, 1); lease.Dispose(); assert.equal(stops, 3);
  const second = activator.Activate(); assert.equal(starts, 3); second.Dispose(); activator.Dispose();
});

test('failed initial activation is retryable and disposes partially created resources', () => {
  const activator = new ViewModelActivator(); let cleaned = 0, fail = true;
  WhenActivated(activator, bag => { bag.Add(() => cleaned++); if (fail) throw Error('fail'); });
  assert.throws(() => activator.Activate(), /fail/); assert.equal(cleaned, 1); assert.equal(activator.ReferenceCount, 0);
  fail = false; const lease = activator.Activate(); lease.Dispose(); assert.equal(cleaned, 2);
});

test('collection edits are atomic, immutable, nested, and rollback exceptions', () => {
  const list = new ObservableCollection([1, 2]); const batches: any[] = [], counts: number[] = [];
  list.CollectionChanged.subscribe(batch => batches.push(batch)); list.CountChanged.subscribe(count => counts.push(count));
  const initial = list.Items;
  list.Edit(items => { items.Add(3); items.Edit(nested => nested.Move(0, 2)); items.SetAt(0, 8); });
  assert.deepEqual(list.Items, [8, 3, 1]); assert.deepEqual(initial, [1, 2]); assert.equal(batches.length, 1);
  assert.deepEqual(batches[0].map((change: any) => change.Reason), ['add', 'move', 'replace']);
  assert.deepEqual(counts, [2, 3]); assert(Object.isFrozen(list.Items));
  assert.throws(() => list.Edit(items => { items.Clear(); items.Add(9); throw Error('rollback'); }), /rollback/);
  assert.deepEqual(list.Items, [8, 3, 1]); assert.equal(batches.length, 1);
  assert.throws(() => list.RemoveAt(-1), RangeError); assert.throws(() => list.RemoveRange(0, 9), RangeError);
  list.Edit(items => { items.Add(5); try { items.Edit(nested => { nested.Clear(); throw Error(); }); } catch {} items.Add(6); });
  assert.deepEqual(list.Items, [8, 3, 1, 5, 6]); list.Dispose(); assert.throws(() => list.Add(10), /disposed/);
});

test('derived lists track live property changes, filters, ordering and unsubscription', () => {
  const a = { Name: 'a', Score: 2, Changed: new Subject<void>() }, b = { Name: 'b', Score: 1, Changed: new Subject<void>() };
  const source = new ObservableCollection([a, b]);
  const derived = new BindableDerivedList(source, { filter: item => item.Score > 0, comparer: (a, b) => a.Score - b.Score });
  assert.deepEqual(derived.Items, [b, a]);
  a.Score = 0; a.Changed.next(); assert.deepEqual(derived.Items, [b]);
  source.Remove(b); assert.deepEqual(derived.Items, []); assert.equal(b.Changed.observed, false);
  derived.SetFilter(() => true); assert.deepEqual(derived.Items, [a]);
  derived.Dispose(); assert.equal(a.Changed.observed, false);
});

test('locator supports contracts, registration disposal, singleton retry and cycle detection', () => {
  const locator = new ServiceLocator(), token = Symbol('service'); let created = 0;
  locator.Register(() => ({ id: ++created }), token);
  assert.notEqual(locator.GetService(token), locator.GetService(token));
  const registration = locator.RegisterLazySingleton(() => ({ id: ++created }), token);
  assert.equal(locator.GetService(token), locator.GetService(token));
  locator.RegisterConstant('contract', token, 'special'); assert.equal(locator.GetService(token, 'special'), 'contract');
  assert.equal(locator.GetServices(token).length, 2); registration.Dispose(); assert.equal(locator.GetServices(token).length, 1);
  const retry = Symbol(); let attempts = 0;
  locator.RegisterLazySingleton(() => { if (++attempts === 1) throw Error('retry'); return 5; }, retry);
  assert.throws(() => locator.GetService(retry), /retry/); assert.equal(locator.GetService(retry), 5);
  const circular = Symbol(); locator.Register(() => locator.GetService(circular), circular);
  assert.throws(() => locator.GetService(circular), /Circular/);
  locator.UnregisterAll(token); assert.equal(locator.HasRegistration(token), false); assert.equal(locator.GetService(token, 'special'), 'contract');
});

test('view locator resolves subclass and contract registrations with reversible overrides', () => {
  class Base {} class Child extends Base {}
  const locator = new ViewLocator(); locator.Register(Base, vm => ({ kind: 'base', vm }));
  const vm = new Child(); assert.equal(locator.ResolveView(vm)?.kind, 'base');
  const override = locator.Register(Child, () => ({ kind: 'child' })); assert.equal(locator.ResolveView(vm)?.kind, 'child');
  locator.Register(Child, () => ({ kind: 'compact' }), 'compact'); assert.equal(locator.ResolveView(vm, 'compact')?.kind, 'compact');
  assert.equal(locator.ResolveView(vm, 'missing'), undefined); override.Dispose(); assert.equal(locator.ResolveView(vm)?.kind, 'base');
});

test('message bus separates tokens and contracts and replays only when requested', () => {
  const bus = new MessageBus(), token = Symbol('message'); const live: number[] = [], latest: number[] = [];
  bus.SendMessage(1, token); bus.Listen<number>(token).subscribe(value => live.push(value));
  bus.ListenIncludeLatest<number>(token).subscribe(value => latest.push(value));
  bus.SendMessage(2, token); bus.SendMessage(9, token, 'contract');
  assert.deepEqual(live, [2]); assert.deepEqual(latest, [1, 2]);
  const source = new Subject<number>(); const sourceLease = bus.RegisterMessageSource(source, token);
  source.next(3); sourceLease.Dispose(); source.next(4); assert.deepEqual(live, [2, 3]);
  const errors: unknown[] = []; bus.ThrownExceptions.subscribe(error => errors.push(error));
  bus.RegisterMessageSource(throwError(() => Error('source failed')), token); bus.SendMessage(5, token);
  assert.equal(errors.length, 1); assert.deepEqual(live, [2, 3, 5]);
  bus.Dispose(); assert.equal(source.observed, false); assert.throws(() => bus.SendMessage(6, token), /disposed/);
});

test('router maintains stack, back command state, and enter/leave notifications', async () => {
  const router = new RoutingState(); const screen = { Router: router };
  const first = { HostScreen: screen, UrlPathSegment: 'home' }, second = { HostScreen: screen, UrlPathSegment: 'settings' };
  let entered = 0, left = 0; const canBack: boolean[] = [];
  WhenNavigatedTo(first).subscribe(() => entered++); WhenNavigatedFrom(first).subscribe(() => left++);
  router.NavigateBack.CanExecute.subscribe(value => canBack.push(value));
  await firstValueFrom(router.Navigate.Execute(first)); assert.equal(router.CurrentViewModelValue, first);
  await firstValueFrom(router.Navigate.Execute(second)); assert.equal(router.CurrentPath, 'home/settings');
  await firstValueFrom(router.NavigateBack.Execute()); assert.equal(router.CurrentViewModelValue, first);
  assert.equal(entered, 2); assert.equal(left, 1); assert(canBack.includes(true)); assert.equal(canBack.at(-1), false);
  await firstValueFrom(router.NavigateAndReset.Execute(second)); assert.equal(router.NavigationStack.Count, 1); router.Dispose();
});

test('persistence drivers isolate snapshots and surface malformed JSON and storage failures', () => {
  const memory = new InMemorySuspensionDriver<{ count: number }>(); const original = { count: 1 }; memory.SaveState(original);
  original.count = 9; const loaded = memory.LoadState()!; assert.equal(loaded.count, 1); loaded.count = 5; assert.equal(memory.LoadState()!.count, 1);
  memory.InvalidateState(); assert.equal(memory.LoadState(), null);
  const data = new Map<string, string>();
  const storage = { getItem: (key: string) => data.get(key) ?? null, setItem: (key: string, value: string) => { data.set(key, value); }, removeItem: (key: string) => { data.delete(key); } };
  const driver = new LocalStorageSuspensionDriver({ storage, key: 'test' }); driver.SaveState({ value: 2 }); assert.deepEqual(driver.LoadState(), { value: 2 });
  data.set('test', 'bad json'); assert.throws(() => driver.LoadState(), SyntaxError);
  const broken = new LocalStorageSuspensionDriver({ storage: { ...storage, setItem() { throw Error('quota'); } } }); assert.throws(() => broken.SaveState({}), /quota/);
});

test('suspension host saves serially, disposes persistence leases and recovers load failures', async () => {
  const host = new SuspensionHost(() => ({ count: 0 })); const saves: number[] = []; let released = 0;
  let resolveSave: (() => void) | undefined;
  const setup = host.SetupDefaultSuspendResume({ LoadState() { throw Error('corrupt'); }, SaveState(state) { saves.push(state.count); return new Promise<void>(resolve => { resolveSave = resolve; }); }, InvalidateState() {} });
  const errors: unknown[] = []; host.ThrownExceptions.subscribe(error => errors.push(error)); host.IsResuming.next();
  assert.deepEqual(host.AppState, { count: 0 }); assert.equal(errors.length, 1);
  host.AppState = { count: 3 }; host.ShouldPersistState.next(Disposable.Create(() => released++));
  assert.deepEqual(saves, [3]); assert.equal(released, 0); resolveSave!(); await delay(); assert.equal(released, 1);
  host.ShouldPersistState.next(Disposable.Create(() => released++)); setup.Dispose(); assert.equal(released, 2); host.Dispose();
});

test('AutoPersist debounces, serializes, exposes failures, flushes, retries and cancels active observables', async () => {
  const item = { Changed: new Subject<void>(), count: 0 }; const saved: number[] = [];
  const persistence = AutoPersist(item, state => { saved.push(state.count); }, { throttleMs: 5 });
  item.count = 1; item.Changed.next(); item.count = 2; item.Changed.next(); await delay(15);
  assert.deepEqual(saved, [2]); item.count = 3; item.Changed.next(); await persistence.Flush(); assert.deepEqual(saved, [2, 3]);
  persistence.Dispose(); item.count = 4; item.Changed.next(); await delay(10); assert.deepEqual(saved, [2, 3]);
  let shouldFail = true; const errors: unknown[] = [];
  const retry = AutoPersist(item, () => { if (shouldFail) throw Error('save failed'); }, { throttleMs: 100 }); retry.Errors.subscribe(error => errors.push(error));
  retry.Trigger(); await assert.rejects(retry.Flush(), /save failed/); assert.equal(errors.length, 1);
  shouldFail = false; retry.Trigger(); await retry.Flush(); retry.Dispose();
  let canceled = false;
  const pending = AutoPersist(item, () => new Observable(() => () => { canceled = true; }), { throttleMs: 100 });
  pending.Trigger(); const flush = pending.Flush(); await delay(); pending.Dispose(); await flush; assert.equal(canceled, true);
});


test('ordered comparers compose keys and reverse individual directions', () => {
  const items = [{ group: 2, name: 'a' }, { group: 1, name: 'a' }, { group: 1, name: 'b' }];
  const ordering = OrderedComparer.OrderBy((item: any) => item.group).ThenByDescending(item => item.name);
  assert.deepEqual(items.sort(ordering.Compare).map(item => `${item.group}${item.name}`), ['1b', '1a', '2a']);
});

test('ActOnEveryObject owns duplicate object subscriptions until the final reference leaves', () => {
  const a = {}, b = {}; const source = new ObservableCollection([a, a]); let created = 0, cleaned = 0;
  const observer = ActOnEveryObject(source, () => { created++; return () => cleaned++; });
  assert.equal(created, 1); source.Remove(a); assert.equal(cleaned, 0);
  source.Add(b); assert.equal(created, 2); source.Remove(a); assert.equal(cleaned, 1);
  observer.Dispose(); observer.Dispose(); assert.equal(cleaned, 2);
});

test('AutoPersistCollection starts and removes per-object persistence subscriptions', async () => {
  const a = { Changed: new Subject<void>(), value: 'a' }, b = { Changed: new Subject<void>(), value: 'b' };
  const source = new ObservableCollection([a]); const saved: string[] = [];
  const observer = AutoPersistCollection(source, item => { saved.push(item.value); }, { throttleMs: 100 });
  a.Changed.next(); source.Add(b); b.Changed.next(); await observer.Flush(); assert.deepEqual(saved, ['a', 'b']);
  source.Remove(a); assert.equal(a.Changed.observed, false); a.Changed.next(); await observer.Flush(); assert.deepEqual(saved, ['a', 'b']);
  observer.Dispose(); assert.equal(b.Changed.observed, false);
});

test('suspension disposal releases queued leases and invalidation follows an in-flight save', async () => {
  const host = new SuspensionHost(() => 0); const operations: string[] = []; let release: (() => void) | undefined, cleaned = 0;
  const setup = host.SetupDefaultSuspendResume({ LoadState: () => null, SaveState() { operations.push('save'); return new Promise<void>(resolve => { release = resolve; }); }, InvalidateState() { operations.push('invalidate'); } });
  host.AppState = 4; host.ShouldPersistState.next(Disposable.Create(() => cleaned++)); host.ShouldInvalidateState.next();
  assert.deepEqual(operations, ['save']); release!(); await delay(); assert.deepEqual(operations, ['save', 'invalidate']); assert.equal(host.AppState, null);
  host.AppState = 5; host.ShouldPersistState.next(Disposable.Create(() => cleaned++)); host.ShouldPersistState.next(Disposable.Create(() => cleaned++));
  setup.Dispose(); assert.equal(cleaned, 3); host.Dispose();
});

test('navigation callback scope survives being current and is disposed upon departure', async () => {
  const router = new RoutingState(), screen = { Router: router }; let entered = 0, cleaned = 0;
  const first = { HostScreen: screen, UrlPathSegment: 'first' }, second = { HostScreen: screen, UrlPathSegment: 'second' };
  const scope = WhenNavigatedTo(first, () => { entered++; return () => cleaned++; });
  await firstValueFrom(router.Navigate.Execute(first)); await firstValueFrom(router.Navigate.Execute(second)); assert.equal(cleaned, 1);
  await firstValueFrom(router.NavigateBack.Execute()); assert.equal(entered, 2); scope.Dispose(); assert.equal(cleaned, 2); router.Dispose();
});


test('singleton view registrations own cached views until their last registration is released', () => {
  class Model {} const locator = new ViewLocator(); let disposed = 0, created = 0;
  const shared = { Dispose() { disposed++; } };
  const first = locator.RegisterSingleton(Model, () => { created++; return shared; });
  const second = locator.RegisterSingleton(Model, () => { created++; return shared; }, 'other');
  const model = new Model(); assert.equal(locator.ResolveView(model), shared); assert.equal(locator.ResolveView(model), shared);
  assert.equal(created, 1); assert(locator.IsSingletonView(shared)); locator.ResolveView(model, 'other');
  first.Dispose(); assert.equal(disposed, 0); assert(locator.IsSingletonView(shared));
  locator.Clear(); assert.equal(disposed, 1); assert.equal(locator.IsSingletonView(shared), false); second.Dispose(); assert.equal(disposed, 1);
});
