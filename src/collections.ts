import { BehaviorSubject, Observable, Subject, Subscription } from 'rxjs';
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
  get ItemsChanged(): Observable<readonly T[]> { return this.snapshots.asObservable(); }
  get CountChanged(): Observable<number> { return this.counts.asObservable(); }
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
    if (this.editDepth === 0) this.publish();
  }
  private publish(): void {
    if (!this.pending.length) return;
    const batch = Object.freeze(this.pending.splice(0));
    const snapshot = Object.freeze([...this.values]);
    this.snapshots.next(snapshot);
    if (this.counts.value !== snapshot.length) this.counts.next(snapshot.length);
    this.changes.next(batch);
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
      const subscription = this.changes.subscribe(subscriber);
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
  /** Defaults to each item's Changed observable, when available. */
  observeItem?: (item: T) => Observable<unknown> | undefined;
}
/** A disposable projection. Live item refresh rebuilds filtering and stable sorting. */
export class BindableDerivedList<T> implements Iterable<T>, IDisposable {
  private readonly result = new ObservableCollection<T>();
  private readonly subscription: Subscription;
  private itemSubscriptions = new Subscription();
  private options: DerivedListOptions<T>;
  private disposed = false;
  constructor(readonly Source: ObservableCollection<T>, options: DerivedListOptions<T> = {}) {
    this.options = options;
    this.subscription = Source.ItemsChanged.subscribe(() => { this.watchItems(); this.Refresh(); });
  }
  get Count(): number { return this.result.Count; }
  get Items(): readonly T[] { return this.result.Items; }
  get ItemsChanged(): Observable<readonly T[]> { return this.result.ItemsChanged; }
  get CountChanged(): Observable<number> { return this.result.CountChanged; }
  get CollectionChanged(): Observable<ChangeSet<T>> { return this.result.CollectionChanged; }
  GetAt(index: number): T { return this.result.GetAt(index); }
  ToArray(): T[] { return this.result.ToArray(); }
  [Symbol.iterator](): Iterator<T> { return this.result[Symbol.iterator](); }
  Connect(): Observable<ChangeSet<T>> { return this.result.Connect(); }
  SetFilter(filter?: (item: T) => boolean): void { this.options = { ...this.options, filter }; this.Refresh(); }
  SetComparer(comparer?: (left: T, right: T) => number): void { this.options = { ...this.options, comparer }; this.Refresh(); }
  private watchItems(): void {
    this.itemSubscriptions.unsubscribe(); this.itemSubscriptions = new Subscription();
    for (const item of new Set(this.Source.Items)) {
      const stream = this.options.observeItem ? this.options.observeItem(item) : (item as { Changed?: Observable<unknown> } | null)?.Changed;
      if (stream && typeof stream.subscribe === 'function') this.itemSubscriptions.add(stream.subscribe(() => this.Refresh()));
    }
  }
  Refresh(): void {
    if (this.disposed) return;
    let items = this.Source.ToArray();
    if (this.options.filter) items = items.filter(this.options.filter);
    if (this.options.comparer) items.sort(this.options.comparer);
    this.result.Reset(items);
  }
  Dispose(): void {
    if (this.disposed) return; this.disposed = true;
    this.subscription.unsubscribe(); this.itemSubscriptions.unsubscribe(); this.result.Dispose();
  }
  unsubscribe(): void { this.Dispose(); }
}

export function ToObservableCollection<T>(source: Observable<Iterable<T>>, target = new ObservableCollection<T>()): { Collection: ObservableCollection<T>; Subscription: IDisposable } {
  const subscription = source.subscribe(items => target.Reset(items));
  return { Collection: target, Subscription: Disposable.Create(() => subscription.unsubscribe()) };
}

/** Own one resource per distinct object; duplicate references retain it until the last removal. */
export function ActOnEveryObject<T>(collection: ObservableCollection<T>, onAdded: (item: T) => DisposableLike | void, onRemoved?: (item: T) => void): IDisposable {
  const resources = new Map<T, DisposableLike>();
  const subscription = collection.ItemsChanged.subscribe(items => {
    const current = new Set(items);
    for (const [item, resource] of resources) if (!current.has(item)) { resources.delete(item); dispose(resource); onRemoved?.(item); }
    for (const item of current) if (!resources.has(item)) resources.set(item, onAdded(item) || undefined);
  });
  return Disposable.Create(() => {
    subscription.unsubscribe(); const errors: unknown[] = [];
    for (const [item, resource] of resources) { try { dispose(resource); onRemoved?.(item); } catch (error) { errors.push(error); } }
    resources.clear(); if (errors.length) throw new AggregateError(errors, 'Collection resource disposal failed');
  });
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
