import { BehaviorSubject, Observable, ReplaySubject, Subject, Subscription, NEVER, share, of } from 'rxjs';
import { AutoRefreshOnObservable, Filter, Sort, ChangeSet as DynamicChangeSet, WhenPropertyChanged, applyChanges, type ChangeSet as DynamicChangeSetType } from '@wieslawsoltes/dynamicdataweb';
import { ApplyDynamicDataChanges, BindChangeSet, ConnectDynamicData, ToDynamicDataChangeSet, type DynamicDataSource } from './dynamic-data.js';
import { Disposable, dispose, type IDisposable, type DisposableLike } from './disposables.js';

export type CollectionChangeReason = 'add' | 'remove' | 'replace' | 'move' | 'reset' | 'refresh';
export interface CollectionChange<T> {
  readonly Reason: CollectionChangeReason;
  readonly Index: number;
  readonly Items: readonly T[];
  readonly PreviousItems?: readonly T[];
  readonly PreviousIndex?: number;
}
export type ChangeSet<T> = readonly CollectionChange<T>[];

/** A list with synchronous mutation, immutable snapshots and atomic change batches. */
export class ObservableCollection<T> implements Iterable<T>, IDisposable {
  protected values: T[];
  private readonly changes = new Subject<ChangeSet<T>>();
  private readonly snapshots: BehaviorSubject<readonly T[]>;
  private readonly counts: BehaviorSubject<number>;
  private editDepth = 0;
  private publishing = false;
  private revision = 0;
  private pending: CollectionChange<T>[] = [];
  private disposed = false;
  constructor(items: Iterable<T> = []) {
    this.values = Array.from(items);
    this.snapshots = new BehaviorSubject(Object.freeze([...this.values]));
    this.counts = new BehaviorSubject(this.values.length);
  }
  get Count(): number { return this.values.length; }
  get length(): number { return this.Count; }
  get Items(): readonly T[] { return this.snapshots.value; }
  get ItemsChanged(): Observable<readonly T[]> { return new Observable(observer => { if (this.disposed) { observer.next(this.Items); observer.complete(); return; } return this.snapshots.subscribe(observer); }); }
  get CountChanged(): Observable<number> { return new Observable(observer => { if (this.disposed) { observer.next(this.Count); observer.complete(); return; } return this.counts.subscribe(observer); }); }
  get CollectionChanged(): Observable<ChangeSet<T>> { return this.changes.asObservable(); }
  get IsDisposed(): boolean { return this.disposed; }
  [Symbol.iterator](): Iterator<T> { return this.values[Symbol.iterator](); }
  GetAt(index: number): T { this.checkIndex(index); return this.values[index]!; }
  IndexOf(item: T): number { return this.values.indexOf(item); }
  Contains(item: T): boolean { return this.values.includes(item); }
  ToArray(): T[] { return [...this.values]; }
  private checkIndex(index: number, allowEnd = false): void {
    if (!Number.isInteger(index) || index < 0 || index >= this.values.length + (allowEnd ? 1 : 0)) throw new RangeError(`Invalid collection index: ${index}`);
  }
  private checkAlive(): void { if (this.disposed) throw new Error('Collection is disposed'); }
  private record(change: CollectionChange<T>): void {
    this.pending.push(Object.freeze(change));
    if (this.editDepth === 0 && !this.publishing) this.publish();
  }
  private publish(): void {
    if (this.publishing || !this.pending.length) return;
    this.publishing = true;
    try {
      // Mutations made by observers queue their publication after the current immutable batch.
      while (this.pending.length) {
        const batch = Object.freeze(this.pending.splice(0));
        this.revision++;
        const snapshot = Object.freeze([...this.values]);
        this.snapshots.next(snapshot);
        if (this.counts.value !== snapshot.length) this.counts.next(snapshot.length);
        this.changes.next(batch);
      }
    } finally { this.publishing = false; }
  }

  Add(item: T): void { this.Insert(this.Count, item); }
  AddRange(items: Iterable<T>): void {
    this.checkAlive(); const added = Array.from(items); if (!added.length) return;
    const index = this.Count; this.values.push(...added);
    this.record({ Reason: 'add', Index: index, Items: Object.freeze(added) });
  }
  Insert(index: number, item: T): void {
    this.checkAlive(); this.checkIndex(index, true); this.values.splice(index, 0, item);
    this.record({ Reason: 'add', Index: index, Items: Object.freeze([item]) });
  }
  Remove(item: T): boolean {
    this.checkAlive(); const index = this.IndexOf(item); if (index < 0) return false;
    this.RemoveAt(index); return true;
  }
  RemoveAt(index: number): T {
    this.checkAlive(); this.checkIndex(index); const item = this.values.splice(index, 1)[0]!;
    this.record({ Reason: 'remove', Index: index, Items: Object.freeze([item]) }); return item;
  }
  RemoveRange(index: number, count: number): void {
    this.checkAlive(); this.checkIndex(index, true);
    if (!Number.isInteger(count) || count < 0 || index + count > this.Count) throw new RangeError('Invalid collection range');
    if (!count) return;
    this.record({ Reason: 'remove', Index: index, Items: Object.freeze(this.values.splice(index, count)) });
  }
  RemoveAll(predicate: (item: T) => boolean): number {
    let removed = 0; this.Edit(list => { for (let i = list.Count - 1; i >= 0; i--) if (predicate(list.GetAt(i))) { list.RemoveAt(i); removed++; } }); return removed;
  }
  SetAt(index: number, item: T): void {
    this.checkAlive(); this.checkIndex(index); const previous = this.values[index]!;
    if (Object.is(previous, item)) return; this.values[index] = item;
    this.record({ Reason: 'replace', Index: index, Items: Object.freeze([item]), PreviousItems: Object.freeze([previous]) });
  }
  Move(oldIndex: number, newIndex: number): void {
    this.checkAlive(); this.checkIndex(oldIndex); this.checkIndex(newIndex); if (oldIndex === newIndex) return;
    const item = this.values.splice(oldIndex, 1)[0]!; this.values.splice(newIndex, 0, item);
    this.record({ Reason: 'move', Index: newIndex, PreviousIndex: oldIndex, Items: Object.freeze([item]) });
  }
  Clear(): void { this.Reset([]); }
  Reset(items: Iterable<T>): void {
    this.checkAlive(); const next = Array.from(items); const previous = this.values;
    if (next.length === previous.length && next.every((item, i) => Object.is(item, previous[i]))) return;
    this.values = next;
    this.record({ Reason: 'reset', Index: 0, Items: Object.freeze([...next]), PreviousItems: Object.freeze([...previous]) });
  }
  Refresh(item?: T): void {
    this.checkAlive(); const index = arguments.length ? this.IndexOf(item as T) : 0;
    if (index < 0) return;
    this.record({ Reason: 'refresh', Index: index, Items: Object.freeze(arguments.length ? [item as T] : [...this.values]) });
  }
  /** Refresh a particular occurrence, including duplicate references. */
  RefreshAt(index: number): void { this.checkAlive(); this.checkIndex(index); this.record({ Reason: 'refresh', Index: index, Items: Object.freeze([this.values[index]!]) }); }
  /** Native DynamicData binding hook; Connect continues to expose the legacy protocol. */
  ApplyChanges<K = unknown>(changes: DynamicChangeSetType<T, K>): void { ApplyDynamicDataChanges(this, changes); }
  /** Nested edits produce one batch. An exception restores that edit's pre-mutation state. */
  Edit(action: (collection: this) => void): void {
    this.checkAlive(); const before = [...this.values], pendingStart = this.pending.length;
    this.editDepth++;
    try { action(this); }
    catch (error) { this.values = before; this.pending.splice(pendingStart); throw error; }
    finally { this.editDepth--; if (!this.editDepth) this.publish(); }
  }
  Connect(): Observable<ChangeSet<T>> {
    return new Observable(subscriber => {
      const revision = this.revision;
      if (this.disposed) { subscriber.next(Object.freeze([{ Reason: 'reset' as const, Index: 0, Items: this.Items }])); subscriber.complete(); return; }
      const subscription = this.changes.subscribe({ next: batch => { if (this.revision > revision) subscriber.next(batch); }, error: error => subscriber.error(error), complete: () => subscriber.complete() });
      if (!subscriber.closed) subscriber.next(Object.freeze([{ Reason: 'reset' as const, Index: 0, Items: this.Items }]));
      return subscription;
    });
  }
  Dispose(): void {
    if (this.disposed) return; this.disposed = true;
    this.changes.complete(); this.snapshots.complete(); this.counts.complete();
  }
  unsubscribe(): void { this.Dispose(); }
  add(item: T): void { this.Add(item); }
  remove(item: T): boolean { return this.Remove(item); }
  edit(action: (collection: this) => void): void { this.Edit(action); }
}

export interface DerivedListOptions<T> {
  filter?: (item: T) => boolean;
  comparer?: (left: T, right: T) => number;
  filterObservable?: Observable<(item: T) => boolean>;
  comparerObservable?: Observable<((left: T, right: T) => number) | undefined>;
  /** Defaults to DynamicData property notifications, including ReactiveObject.Changed. */
  observeItem?: (item: T) => Observable<unknown> | undefined;
}
/** Incremental DynamicData projection; unaffected objects keep their existing property subscriptions. */
export class BindableDerivedList<T> implements Iterable<T>, IDisposable {
  private readonly result = new ObservableCollection<T>();
  private readonly filtered = new ObservableCollection<T>();
  private readonly subscription = new Subscription();
  private readonly errors = new ReplaySubject<unknown>(1);
  private readonly filter: BehaviorSubject<(item: T) => boolean>;
  private readonly comparer: BehaviorSubject<(left: T, right: T) => number>;
  private readonly refresh = new Subject<void>();
  private sorted?: Subscription;
  private comparison?: (left: T, right: T) => number;
  private disposed = false;
  constructor(readonly Source: DynamicDataSource<T>, options: DerivedListOptions<T> = {}) {
    this.filter = new BehaviorSubject(options.filter ?? (() => true));
    this.comparison = options.comparer;
    this.comparer = new BehaviorSubject(options.comparer ?? (() => 0));
    const streams = new Map<T, Observable<unknown>>();
    const observe = (item: T): Observable<unknown> => {
      let stream = streams.get(item);
      if (!stream) {
        const selected = options.observeItem ? options.observeItem(item)
          : item != null && (typeof item === 'object' || typeof item === 'function') ? WhenPropertyChanged(item, undefined as never, false) : NEVER;
        stream = (selected ?? NEVER).pipe(share({ resetOnRefCountZero: () => { streams.delete(item); return of(undefined); } })); streams.set(item, stream);
      }
      return stream;
    };
    const input = ConnectDynamicData(Source).pipe(AutoRefreshOnObservable(observe), Filter(this.filter, this.refresh));
    const binding = BindChangeSet(input, this.filtered);
    this.subscription.add(() => binding.Dispose());
    this.subscription.add(binding.Errors.subscribe(error => this.errors.next(error)));
    this.bindOrdering();
    if (options.filterObservable) this.subscription.add(options.filterObservable.subscribe({ next: value => this.SetFilter(value), error: error => this.errors.next(error) }));
    if (options.comparerObservable) this.subscription.add(options.comparerObservable.subscribe({ next: value => this.SetComparer(value), error: error => this.errors.next(error) }));
    this.subscription.add(() => streams.clear());
  }
  get Count(): number { return this.result.Count; }
  get Items(): readonly T[] { return this.result.Items; }
  get ItemsChanged(): Observable<readonly T[]> { return this.result.ItemsChanged; }
  get CountChanged(): Observable<number> { return this.result.CountChanged; }
  get CollectionChanged(): Observable<ChangeSet<T>> { return this.result.CollectionChanged; }
  get ThrownExceptions(): Observable<unknown> { return this.errors.asObservable(); }
  get IsDisposed(): boolean { return this.disposed; }
  GetAt(index: number): T { return this.result.GetAt(index); }
  ToArray(): T[] { return this.result.ToArray(); }
  [Symbol.iterator](): Iterator<T> { return this.result[Symbol.iterator](); }
  Connect(): Observable<ChangeSet<T>> { return this.result.Connect(); }
  SetFilter(filter?: (item: T) => boolean): void { if (!this.disposed) this.filter.next(filter ?? (() => true)); }
  SetComparer(comparer?: (left: T, right: T) => number): void {
    if (this.disposed) return;
    const toggle = !!comparer !== !!this.comparison; this.comparison = comparer;
    if (toggle) this.sorted?.unsubscribe();
    this.comparer.next(comparer ?? (() => 0));
    if (toggle) this.bindOrdering();
  }
  private bindOrdering(): void {
    this.sorted?.unsubscribe(); let first = true;
    const source = ToDynamicDataChangeSet(this.filtered);
    const ordered = this.comparison ? source.pipe(Sort(this.comparer, { resetThreshold: Number.POSITIVE_INFINITY })) : source;
    this.sorted = ordered.subscribe({
      next: changes => {
        try {
          // A change of ordering starts a fresh operator subscription, while the target retains rows.
          this.result.ApplyChanges(first ? new DynamicChangeSet([], 'list', applyChanges([], changes)) : changes);
          first = false;
        } catch (error) { this.errors.next(error); }
      }, error: error => this.errors.next(error),
    });
  }
  Refresh(): void { if (!this.disposed) { this.refresh.next(); if (this.comparison) this.comparer.next(this.comparison); } }
  Dispose(): void {
    if (this.disposed) return; this.disposed = true;
    this.subscription.unsubscribe(); this.sorted?.unsubscribe(); this.filter.complete(); this.comparer.complete(); this.refresh.complete();
    this.filtered.Dispose(); this.result.Dispose(); this.errors.complete();
  }
  unsubscribe(): void { this.Dispose(); }
}

export function ToObservableCollection<T>(source: Observable<Iterable<T>>, target = new ObservableCollection<T>()): { Collection: ObservableCollection<T>; Subscription: IDisposable } {
  const subscription = source.subscribe(items => target.Reset(items));
  return { Collection: target, Subscription: Disposable.Create(() => subscription.unsubscribe()) };
}

export interface IObjectLifecycleSubscription extends IDisposable {
  readonly Errors: Observable<unknown>;
  unsubscribe(): void;
}
/** Own one resource per distinct object; duplicates retain it until their last removal. */
export function ActOnEveryObject<T>(collection: DynamicDataSource<T>, onAdded: (item: T) => DisposableLike | void, onRemoved?: (item: T) => void): IObjectLifecycleSubscription {
  const resources = new Map<T, DisposableLike>(), errors = new ReplaySubject<unknown>(1), scope = new Subscription();
  const state = new ObservableCollection<T>(); let disposed = false, processing = false, terminated = false;
  const pending: DynamicChangeSetType<T>[] = [];
  const release = (item: T, resource: DisposableLike, failures: unknown[]) => {
    resources.delete(item);
    try { dispose(resource); } catch (error) { failures.push(error); }
    try { onRemoved?.(item); } catch (error) { failures.push(error); }
  };
  const cleanup = (): unknown[] => { const failures: unknown[] = []; for (const [item, resource] of resources) release(item, resource, failures); return failures; };
  const report = (failures: unknown[]) => { if (failures.length) errors.next(new AggregateError(failures, 'Collection resource disposal failed')); };
  const finish = () => { report(cleanup()); state.Dispose(); errors.complete(); };
  scope.add(ConnectDynamicData(collection).subscribe({
    next(changes) {
      if (disposed || terminated) return;
      pending.push(changes); if (processing) return; processing = true;
      try {
        while (pending.length) {
        state.ApplyChanges(pending.shift()!); const current = new Set(state.Items), failures: unknown[] = [];
        for (const [item, resource] of resources) if (!current.has(item)) release(item, resource, failures);
        for (const item of current) if (!resources.has(item)) { try { resources.set(item, onAdded(item) || undefined); } catch (error) { failures.push(error); } }
        report(failures);
        }
      } catch (error) { errors.next(error); report(cleanup()); } finally { processing = false; if (terminated) finish(); }
    },
    error(error) { errors.next(error); terminated = true; if (!processing) finish(); },
    complete() { terminated = true; if (!processing) finish(); },
  }));
  return { Errors: errors.asObservable(),
    Dispose() { if (disposed) return; disposed = true; scope.unsubscribe(); const failures = cleanup(); state.Dispose(); errors.complete(); if (failures.length) throw new AggregateError(failures, 'Collection resource disposal failed'); },
    unsubscribe() { this.Dispose(); },
  };
}

export function ObserveCollectionChanges<T>(collection: ObservableCollection<T>): Observable<ChangeSet<T>> { return collection.Connect(); }
export function WhenCountChanged<T>(collection: ObservableCollection<T>): Observable<number> { return collection.CountChanged; }
export type ReactiveChange<T> = CollectionChange<T>;
export type ReactiveChangeSet<T> = ChangeSet<T>;
export type Comparer<T> = (left: T, right: T) => number;
/** Composable stable-order keys usable with native array sort and derived collections. */
export class OrderedComparer<T> {
  private constructor(private readonly compare: Comparer<T>) {}
  static OrderBy<T, TKey>(selector: (item: T) => TKey, comparer?: Comparer<TKey>): OrderedComparer<T> {
    const compare = comparer ?? ((left: TKey, right: TKey) => left < right ? -1 : left > right ? 1 : 0);
    return new OrderedComparer((left, right) => compare(selector(left), selector(right)));
  }
  static OrderByDescending<T, TKey>(selector: (item: T) => TKey, comparer?: Comparer<TKey>): OrderedComparer<T> {
    const ascending = OrderedComparer.OrderBy(selector, comparer); return new OrderedComparer((left, right) => -ascending.Compare(left, right));
  }
  ThenBy<TKey>(selector: (item: T) => TKey, comparer?: Comparer<TKey>): OrderedComparer<T> {
    const next = OrderedComparer.OrderBy(selector, comparer); return new OrderedComparer((left, right) => this.Compare(left, right) || next.Compare(left, right));
  }
  ThenByDescending<TKey>(selector: (item: T) => TKey, comparer?: Comparer<TKey>): OrderedComparer<T> {
    const next = OrderedComparer.OrderByDescending(selector, comparer); return new OrderedComparer((left, right) => this.Compare(left, right) || next.Compare(left, right));
  }
  Compare = (left: T, right: T): number => this.compare(left, right);
}
