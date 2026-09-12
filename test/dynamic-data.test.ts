import test from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorSubject, Observable, Subject, firstValueFrom, of } from 'rxjs';
import { SourceList, SourceCache, AutoRefresh, Filter, Sort, Bind, ChangeSet, ListChange, applyChanges } from '@wieslawsoltes/dynamicdataweb';
import { ObservableCollection, BindableDerivedList, ActOnEveryObject } from '../dist/collections.js';
import { ToDynamicDataChangeSet, ToReactiveCollection, BindChangeSet } from '../dist/dynamic-data.js';
import { ReactiveObject, DefineReactiveProperty } from '../dist/reactive-object.js';
import { AutoPersistCollection } from '../dist/persistence.js';

class Row extends ReactiveObject {
  constructor(readonly Id: string, score: number) { super(); DefineReactiveProperty(this, 'Score', score); }
  declare Score: number;
}

test('legacy batches convert losslessly to native list changes without changing Connect or rollback', () => {
  const a = {}, b = {}; const source = new ObservableCollection([a, b, a]);
  const state: unknown[] = [], batches: any[] = [];
  const subscription = ToDynamicDataChangeSet(source).subscribe(changes => { applyChanges(state, changes); batches.push(changes); });
  assert.deepEqual(state, [a, b, a]); assert.equal(batches[0].kind, 'list');
  source.Edit(list => {
    list.Move(2, 0); list.SetAt(1, b); list.RefreshAt(2);
    try { list.Edit(inner => { inner.Clear(); throw Error('inner'); }); } catch {}
  });
  assert.equal(batches.length, 2); assert.deepEqual(state, source.Items);
  assert.deepEqual(batches[1].map((change: any) => change.reason), ['move', 'replace', 'refresh']);
  let legacy: any; source.Connect().subscribe(value => legacy = value).unsubscribe();
  assert.equal(legacy[0].Reason, 'reset'); assert.equal(legacy.kind, undefined);
  source.Reset([undefined, null, a] as any); assert.deepEqual(state, [undefined, null, a]);
  source.Clear(); assert.deepEqual(state, []); subscription.unsubscribe(); source.Dispose();
});

test('native list binding preserves duplicate indices, atomic batches, moves and refresh occurrences', () => {
  const a = {}, b = {}; const source = new SourceList([a, b, a]); const binding = ToReactiveCollection(source);
  const batches: any[] = []; binding.Collection.CollectionChanged.subscribe(batch => batches.push(batch));
  source.Edit(list => { list.Move(2, 0); list.RemoveAt(1); list.RefreshAt(1); });
  assert.deepEqual(binding.Collection.Items, [a, b]); assert.equal(batches.length, 1);
  assert.deepEqual(batches[0].map((c: any) => c.Reason), ['move', 'remove', 'refresh']);
  assert.equal(batches[0][2].Index, 1); assert(batches[0].every((c: any) => c.Reason !== 'reset'));
  binding.Dispose(); source.Add(a); assert.deepEqual(binding.Collection.Items, [a, b]); source.Dispose();
});

test('cache binding tracks keys when values share references and sorted updates move their entry', () => {
  const same = { Score: 1 }; const cache = new SourceCache<any, string>(item => item.Id);
  cache.AddOrUpdate(same, 'left'); cache.AddOrUpdate(same, 'right');
  const binding = BindChangeSet(cache); assert.deepEqual(binding.Collection.Items, [same, same]);
  const other = { Score: 4 }; cache.AddOrUpdate(other, 'right'); assert.deepEqual(binding.Collection.Items, [same, other]);
  cache.RemoveKey('left'); assert.deepEqual(binding.Collection.Items, [other]); binding.Dispose(); cache.Dispose();
  const orderedCache = new SourceCache<Row, string>(row => row.Id); const a = new Row('a', 1), b = new Row('b', 2);
  const ordered = ToReactiveCollection(orderedCache.Connect().pipe(AutoRefresh('Score'), Sort((a, b) => a.Score - b.Score)));
  orderedCache.AddOrUpdate([a, b]); a.Score = 3; assert.deepEqual(ordered.Collection.Items, [b, a]);
  const replacement = new Row('b', 4); orderedCache.AddOrUpdate(replacement); assert.deepEqual(ordered.Collection.Items, [a, replacement]);
  orderedCache.RemoveKey('a'); assert.deepEqual(ordered.Collection.Items, [replacement]); ordered.Dispose(); orderedCache.Dispose();
});

test('DynamicData Bind uses the native ApplyChanges hook without replacing the legacy Connect shape', () => {
  const source = new SourceList([3, 1, 2]); const target = new ObservableCollection<number>(); const sets: any[] = [];
  const binding = source.Connect().pipe(Sort((a, b) => a - b), Bind(target)).subscribe(set => sets.push(set));
  assert.deepEqual(target.Items, [1, 2, 3]); source.Add(0); assert.deepEqual(target.Items, [0, 1, 2, 3]);
  assert.equal(sets.at(-1).kind, 'list'); binding.unsubscribe(); source.Dispose(); target.Dispose();
});

test('derived lists use native property refresh, predicate streams, comparer streams and stable subscriptions', () => {
  let added = 0, removed = 0;
  const observed = (id: string, score: number) => {
    const events = new Subject<void>();
    return { id, score, events, Changed: new Observable<void>(subscriber => { added++; const sub = events.subscribe(subscriber); return () => { removed++; sub.unsubscribe(); }; }) };
  };
  const a = observed('a', 3), b = observed('b', 1), c = observed('c', 2);
  const source = new SourceList([a, b, a]); const filter = new BehaviorSubject<(item: typeof a) => boolean>(() => true);
  const comparer = new BehaviorSubject<((a: typeof a, b: typeof a) => number) | undefined>(undefined);
  const derived = new BindableDerivedList(source, { filterObservable: filter, comparerObservable: comparer });
  assert.equal(added, 2); assert.deepEqual(derived.Items, [a, b, a]);
  source.Add(c); assert.equal(added, 3); assert.equal(removed, 0);
  source.Move(2, 0); assert.deepEqual(derived.Items, [a, a, b, c]);
  comparer.next((a, b) => a.score - b.score); assert.deepEqual(derived.Items, [b, c, a, a]); assert.equal(added, 3);
  a.score = 0; a.events.next(); assert.deepEqual(derived.Items, [a, a, b, c]);
  filter.next(item => item.score > 0); assert.deepEqual(derived.Items, [b, c]);
  filter.next(() => true); comparer.next(undefined); assert.deepEqual(derived.Items, [a, a, b, c]);
  source.RemoveAt(0); assert.equal(removed, 0); source.RemoveAt(0); assert.equal(removed, 1);
  derived.Dispose(); assert.equal(removed, 3); source.Dispose();
});

test('derived lists accept cache and change-set streams and emit deltas rather than resets', () => {
  const cache = new SourceCache<Row, string>(row => row.Id), a = new Row('a', 1), b = new Row('b', 2);
  cache.AddOrUpdate([a, b]); const derived = new BindableDerivedList(cache.Connect(), { filter: row => row.Score > 0, comparer: (a, b) => a.Score - b.Score });
  const batches: any[] = []; derived.CollectionChanged.subscribe(batch => batches.push(batch));
  a.Score = 4; assert.deepEqual(derived.Items, [b, a]);
  b.Score = -1; assert.deepEqual(derived.Items, [a]);
  assert(batches.length >= 2); assert(batches.flat().every(change => change.Reason !== 'reset'));
  const countBefore = batches.length; a.Score = 5; assert(batches.length > countBefore);
  derived.Dispose(); cache.Dispose();
});

test('derived primitive/null lists preserve source order and explicit manual refresh semantics', () => {
  const source = new ObservableCollection<any>([3, null, 1, 3]); const derived = new BindableDerivedList(source);
  assert.deepEqual(derived.Items, source.Items); source.Move(3, 0); assert.deepEqual(derived.Items, source.Items);
  derived.SetComparer((a, b) => (a ?? 0) - (b ?? 0)); assert.deepEqual(derived.Items, [null, 1, 3, 3]);
  derived.SetComparer(); assert.deepEqual(derived.Items, source.Items); derived.Dispose(); source.Dispose();
});

test('native cache lifecycle holds one resource per object, even across key replacement in one Edit', () => {
  const same = {}, other = {}; const source = new SourceCache<any, string>(item => item.id); const events: string[] = [];
  source.AddOrUpdate(same, 'a'); source.AddOrUpdate(same, 'b');
  const lifetime = ActOnEveryObject(source, item => { events.push(item === same ? 'same+' : 'other+'); return () => events.push(item === same ? 'same-' : 'other-'); });
  source.RemoveKey('a'); assert.deepEqual(events, ['same+']);
  source.Edit(cache => { cache.RemoveKey('b'); cache.AddOrUpdate(same, 'c'); cache.AddOrUpdate(other, 'd'); });
  assert.deepEqual(events, ['same+', 'other+']); source.Dispose(); assert.deepEqual(events, ['same+', 'other+', 'same-', 'other-']); lifetime.Dispose();
});

test('native change-set lifecycle completion and disposal release all resources despite teardown failures', () => {
  const source = new Subject<any>(), a = {}, b = {}; const removed: unknown[] = [], errors: unknown[] = [];
  const lifetime = ActOnEveryObject(source, () => () => { throw Error('cleanup'); }, item => removed.push(item));
  lifetime.Errors.subscribe(error => errors.push(error)); source.next(new ChangeSet([new ListChange('addRange', [a, b], 0)], 'list'));
  source.complete(); assert.deepEqual(removed, [a, b]); assert.equal(errors.length, 1); lifetime.Dispose();
  let entered = 0, exited = 0;
  ActOnEveryObject(of(new ChangeSet([new ListChange('add', a, 0)], 'list')), () => { entered++; return () => exited++; });
  assert.equal(entered, 1); assert.equal(exited, 1);
});

test('AutoPersistCollection supports native cache refresh, duplicate objects, removal cancellation and failures', async () => {
  const a = new Row('a', 1), b = new Row('b', 2), source = new SourceCache<Row, string>(row => row.Id), saved: string[] = [];
  source.AddOrUpdate(a, 'a'); source.AddOrUpdate(a, 'duplicate'); source.AddOrUpdate(b, 'b');
  let fail = true;
  const lifetime = AutoPersistCollection(source, async row => { if (row === b && fail) throw Error('save failed'); saved.push(row.Id); }, { throttleMs: 10000 });
  const errors: unknown[] = []; lifetime.Errors.subscribe(error => errors.push(error));
  a.Score = 2; await lifetime.Flush(); assert.deepEqual(saved, ['a']);
  source.RemoveKey('a'); a.Score = 3; await lifetime.Flush(); assert.deepEqual(saved, ['a', 'a']);
  b.Score = 4; await assert.rejects(lifetime.Flush(), /save failed/); assert.equal(errors.length, 1);
  fail = false; b.Score = 5; await lifetime.Flush(); assert.deepEqual(saved, ['a', 'a', 'b']);
  source.RemoveKey('duplicate'); a.Score = 6; await lifetime.Flush(); assert.equal(saved.length, 3);
  source.Dispose(); b.Score = 7; await lifetime.Flush(); assert.equal(saved.length, 3); lifetime.Dispose();
});

test('AutoPersistCollection reports source errors through Errors and Flush and cancels active observable saves', async () => {
  const source = new Subject<any>(), row = new Row('x', 1); let cancelled = 0;
  let signalStarted!: () => void; const started = new Promise<void>(resolve => signalStarted = resolve);
  const lifetime = AutoPersistCollection(source, () => new Observable(() => { signalStarted(); return () => cancelled++; }), { throttleMs: 10000 });
  const errors: unknown[] = []; lifetime.Errors.subscribe(error => errors.push(error));
  source.next(new ChangeSet([new ListChange('add', row, 0)], 'list')); row.Score = 2;
  const saving = lifetime.Flush(); await started; source.error(Error('source failed'));
  await assert.rejects(saving, /source failed/); assert.equal(cancelled, 1); assert.equal(errors.length, 1); lifetime.Dispose();
});

test('legacy reentrant snapshot observers preserve native batch order and final list contents', () => {
  const source = new ObservableCollection([1]), values: number[] = []; let once = false;
  const scope = ToDynamicDataChangeSet(source).subscribe(batch => applyChanges(values, batch));
  source.ItemsChanged.subscribe(items => { if (items.includes(2) && !once) { once = true; source.Insert(0, 0); } });
  source.Add(2); assert.deepEqual(values, [0, 1, 2]); assert.deepEqual(values, source.Items);
  scope.unsubscribe(); source.Dispose();
});

test('bindings retain final snapshots, stop synchronously on invalid deltas and isolate new cache key models', () => {
  const completed = ToReactiveCollection(of(new ChangeSet([new ListChange('add', 1, 0)], 'list')));
  const snapshots: unknown[] = []; completed.Collection.ItemsChanged.subscribe(items => snapshots.push(items));
  assert.deepEqual(snapshots, [[1]]); assert.equal(completed.IsDisposed, true);
  let delivered = 0, cleaned = 0;
  const invalid = new Observable<any>(observer => {
    while (!observer.closed && delivered < 3) { delivered++; observer.next(new ChangeSet([new ListChange('add', 2, 20)], 'list')); }
    return () => cleaned++;
  });
  const failed = ToReactiveCollection(invalid); assert.equal(delivered, 1); assert.equal(cleaned, 1); assert.equal(failed.IsDisposed, true);
  const errors: unknown[] = []; failed.Errors.subscribe(error => errors.push(error)); assert.equal(errors.length, 1);
  const a = {}, b = {}, cache = new SourceCache<any, number>(() => 0), target = new ObservableCollection([b]);
  cache.AddOrUpdate(a, -0); const first = BindChangeSet(cache, target); assert.deepEqual(target.Items, [a]);
  cache.AddOrUpdate(b, +0); assert.deepEqual(target.Items, [b]); first.Dispose();
  const other = new SourceCache<any, string>(() => 'x'); other.AddOrUpdate(a); const second = BindChangeSet(other, target);
  assert.deepEqual(target.Items, [a]); other.RemoveKey('x'); assert.deepEqual(target.Items, []);
  second.Dispose(); cache.Dispose(); other.Dispose(); target.Dispose();
});

test('reentrant initial collection lifetimes create one resource and complete after callback teardown is registered', () => {
  const source = new ObservableCollection(['a']), events: string[] = []; let once = false;
  const lifetime = ActOnEveryObject(source, item => { events.push(`${item}+`); if (item === 'a' && !once) { once = true; source.Add('b'); } return () => events.push(`${item}-`); });
  lifetime.Dispose(); assert.deepEqual(events, ['a+', 'b+', 'a-', 'b-']); source.Dispose();
  const terminating = new ObservableCollection(['x']); let cleaned = 0;
  ActOnEveryObject(terminating, () => { terminating.Dispose(); return () => cleaned++; }); assert.equal(cleaned, 1);
  const empty = new SourceList<number>(), populated = new ObservableCollection([42]); const binding = BindChangeSet(empty, populated);
  assert.deepEqual(populated.Items, []); binding.Dispose(); empty.Dispose(); populated.Dispose();
});

test('reentrant binding target observers queue updates during the first snapshot', () => {
  const source = new ObservableCollection([1]), target = new ObservableCollection<number>(); let once = false;
  target.ItemsChanged.subscribe(items => { if (items.length && !once) { once = true; source.Add(2); } });
  const binding = BindChangeSet(source, target); assert.deepEqual(target.Items, [1, 2]);
  binding.Dispose(); target.Dispose(); source.Dispose();
});

test('connections created inside snapshot notifications do not duplicate the current change batch', () => {
  const source = new ObservableCollection([1]), values: number[] = []; let connection: any;
  source.ItemsChanged.subscribe(items => { if (items.includes(2) && !connection) connection = ToDynamicDataChangeSet(source).subscribe(batch => applyChanges(values, batch)); });
  source.Add(2); assert.deepEqual(values, [1, 2]); source.Add(3); assert.deepEqual(values, [1, 2, 3]);
  connection.unsubscribe(); source.Dispose();
});

test('reentrant source completion drains changes emitted before its terminal signal', () => {
  const source = new ObservableCollection([1]), target = new ObservableCollection<number>(); let once = false;
  target.ItemsChanged.subscribe(items => { if (items.length && !once) { once = true; source.Add(2); source.Dispose(); } });
  const binding = BindChangeSet(source, target); assert.deepEqual(target.Items, [1, 2]); assert.equal(binding.IsDisposed, true); target.Dispose();
  const lifetimeSource = new ObservableCollection(['a']), events: string[] = [];
  ActOnEveryObject(lifetimeSource, item => { events.push(item + '+'); if (item === 'a') { lifetimeSource.Add('b'); lifetimeSource.Dispose(); } return () => events.push(item + '-'); });
  assert.deepEqual(events, ['a+', 'b+', 'a-', 'b-']);
});
