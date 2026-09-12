import { Observable, ReplaySubject, Subscriber, Subscription, isObservable } from 'rxjs';
import { ChangeSet as DynamicChangeSet, ListChange, type ChangeRecord, type ChangeSet as DynamicChangeSetType } from '@wieslawsoltes/dynamicdataweb';
import { ObservableCollection, type ChangeSet as ReactiveChangeSet } from './collections.js';
import type { IDisposable } from './disposables.js';

// Keep the complete DynamicData surface available without colliding with ReactiveWeb's names.
export * from '@wieslawsoltes/dynamicdataweb';
export * as DynamicData from '@wieslawsoltes/dynamicdataweb';
export type DynamicDataSource<T, K = unknown> = Observable<DynamicChangeSetType<T, K>>
  | { Connect(): Observable<DynamicChangeSetType<T, K>> }
  | { Connect(): Observable<ReactiveChangeSet<T>> };

/** Convert the legacy collection protocol, retaining indices, duplicate occurrences and Edit batches. */
export function ToDynamicDataChangeSet<T>(source: { Connect(): Observable<ReactiveChangeSet<T>> } | Observable<ReactiveChangeSet<T>>): Observable<DynamicChangeSetType<T>> {
  return new Observable(observer => {
    let values: T[] = [];
    const input = new Subscriber<ReactiveChangeSet<T>>({
      next(batch) {
        try {
          const output: ChangeRecord<T>[] = [];
          for (const change of batch) {
            const index = change.Index, items = [...change.Items];
            switch (change.Reason) {
              case 'reset':
                if (values.length) output.push(new ListChange('clear', values, 0));
                if (items.length) output.push(new ListChange('addRange', items, 0));
                values = items; break;
              case 'add': output.push(new ListChange('addRange', items, index)); values.splice(index, 0, ...items); break;
              case 'remove': output.push(new ListChange('removeRange', items, index)); values.splice(index, items.length); break;
              case 'replace':
                for (let offset = 0; offset < items.length; offset++) { output.push(new ListChange('replace', items[offset]!, index + offset, values[index + offset], index + offset)); values[index + offset] = items[offset]!; }
                break;
              case 'move': { const from = change.PreviousIndex!; const moved = values.splice(from, items.length); values.splice(index, 0, ...moved); for (let offset = 0; offset < moved.length; offset++) output.push(new ListChange('move', moved[offset]!, index + offset, undefined, from + offset)); break; }
              case 'refresh': for (let offset = 0; offset < items.length; offset++) output.push(new ListChange('refresh', items[offset]!, index + offset)); break;
            }
          }
          observer.next(new DynamicChangeSet(output, 'list'));
        } catch (error) { observer.error(error); }
      }, error: (error: unknown) => observer.error(error), complete: () => observer.complete(),
    });
    observer.add(input);
    (isObservable(source) ? source : source.Connect()).subscribe(input);
    return input;
  });
}

/** ReactiveUI-style name for converting an ObservableCollection to DynamicData's change sets. */
export const ToObservableChangeSet = ToDynamicDataChangeSet;

/** Connect to native DynamicData sources or normalize the legacy ReactiveWeb protocol. */
export function ConnectDynamicData<T, K = unknown>(source: DynamicDataSource<T, K>): Observable<DynamicChangeSetType<T, K>> {
  if (source instanceof ObservableCollection) return ToDynamicDataChangeSet(source) as Observable<DynamicChangeSetType<T, K>>;
  return new Observable(observer => {
    const scope = new Subscription();
    // Structural legacy collections (including BindableDerivedList) also keep their public Connect shape.
    let legacy: ReplaySubject<ReactiveChangeSet<T>> | undefined;
    const stream = (isObservable(source) ? source : (source.Connect as (predicate?: undefined, suppressEmpty?: boolean) => Observable<DynamicChangeSetType<T, K> | ReactiveChangeSet<T>>)(undefined, false)) as Observable<DynamicChangeSetType<T, K> | ReactiveChangeSet<T>>;
    observer.add(scope);
    const input = new Subscriber<DynamicChangeSetType<T, K> | ReactiveChangeSet<T>>({
      next(batch) {
        if ('kind' in batch) observer.next(batch as DynamicChangeSetType<T, K>);
        else {
          if (!legacy) { legacy = new ReplaySubject<ReactiveChangeSet<T>>(0); scope.add(ToDynamicDataChangeSet(legacy).subscribe(observer as never)); }
          legacy.next(batch as ReactiveChangeSet<T>);
        }
      },
      error(error: unknown) { if (legacy) legacy.error(error); else observer.error(error); },
      complete() { if (legacy) legacy.complete(); else observer.complete(); },
    });
    scope.add(input); stream.subscribe(input);
    return scope;
  });
}

interface BindingState { kind?: 'list' | 'cache'; keys: unknown[]; }
const states = new WeakMap<object, BindingState>();
const sameKey = (left: unknown, right: unknown): boolean => left === right || (left !== left && right !== right);

/** Apply native changes directly. Cache keys identify entries even when multiple keys share one object. */
export function ApplyDynamicDataChanges<T, K>(target: ObservableCollection<T>, changes: DynamicChangeSetType<T, K>): void {
  let state = states.get(target);
  if (!state) { state = { keys: [] }; states.set(target, state); }
  if (state.kind && state.kind !== changes.kind && target.Count) throw new TypeError('Cannot mix list and cache change sets in one binding');
  const previous = { kind: state.kind, keys: [...state.keys] };
  state.kind = changes.kind;
  try {
    target.Edit(list => {
      for (const change of changes) {
        const index = change.currentIndex;
        if (changes.kind === 'cache') {
          const existing = state!.keys.findIndex(key => sameKey(key, change.key));
          switch (change.reason) {
            case 'add': case 'update': case 'replace': {
              if (existing >= 0) {
                list.SetAt(existing, change.current);
                if (index >= 0 && index !== existing) { list.Move(existing, index); const [key] = state!.keys.splice(existing, 1); state!.keys.splice(index, 0, key); }
              } else { const at = index < 0 ? list.Count : index; list.Insert(at, change.current); state!.keys.splice(at, 0, change.key); }
              break;
            }
            case 'remove': if (existing >= 0) { list.RemoveAt(existing); state!.keys.splice(existing, 1); } break;
            case 'move': if (existing >= 0 && index >= 0) { list.Move(existing, index); const [key] = state!.keys.splice(existing, 1); state!.keys.splice(index, 0, key); } break;
            case 'refresh': if (existing >= 0) list.RefreshAt(existing); break;
            case 'clear': list.RemoveRange(0, list.Count); state!.keys = []; break;
          }
        } else {
          const range = Array.isArray(change.range) ? change.range : change.range?.items ?? [];
          const rangeIndex = Array.isArray(change.range) ? index : change.range?.index ?? index;
          switch (change.reason) {
            case 'add': list.Insert(index < 0 ? list.Count : index, change.current); break;
            case 'addRange': { const at = rangeIndex < 0 ? list.Count : rangeIndex; for (let i = 0; i < range.length; i++) list.Insert(at + i, range[i]!); break; }
            case 'remove': { const at = index < 0 ? list.IndexOf(change.current) : index; if (at >= 0) list.RemoveAt(at); break; }
            case 'removeRange': if (rangeIndex >= 0) list.RemoveRange(rangeIndex, range.length); else for (const item of range) list.Remove(item); break;
            case 'clear': list.RemoveRange(0, list.Count); break;
            case 'replace': case 'update': {
              const from = change.previousIndex >= 0 ? change.previousIndex : index >= 0 ? index : list.IndexOf(change.previous as T);
              if (from >= 0) { list.SetAt(from, change.current); if (index >= 0 && index !== from) list.Move(from, index); }
              else list.Insert(index < 0 ? list.Count : index, change.current);
              break;
            }
            case 'move': { const from = change.previousIndex >= 0 ? change.previousIndex : list.IndexOf(change.current); if (from >= 0) list.Move(from, index); break; }
            case 'refresh': { const at = index >= 0 ? index : list.IndexOf(change.current); if (at >= 0) list.RefreshAt(at); break; }
          }
        }
      }
      // Some ordered operators supply authoritative snapshots and keys with move-free deltas.
      if (changes.items) {
        for (let index = 0; index < changes.items.length; index++) {
          const item = changes.items[index]!;
          if (index < list.Count && (changes.kind === 'cache' && changes.keys ? sameKey(state!.keys[index], changes.keys[index]) : Object.is(list.GetAt(index), item))) { list.SetAt(index, item); continue; }
          const at = changes.kind === 'cache' && changes.keys
            ? state!.keys.findIndex(key => sameKey(key, changes.keys![index]))
            : list.ToArray().findIndex((value, offset) => offset >= index && Object.is(value, item));
          if (at < 0) { list.Insert(index, item); if (changes.kind === 'cache') state!.keys.splice(index, 0, changes.keys?.[index]); }
          else { if (at !== index) { list.Move(at, index); if (changes.kind === 'cache') { const [key] = state!.keys.splice(at, 1); state!.keys.splice(index, 0, key); } } list.SetAt(index, item); }
        }
        if (list.Count > changes.items.length) { list.RemoveRange(changes.items.length, list.Count - changes.items.length); state!.keys.length = changes.items.length; }
      }
    });
  } catch (error) { state.kind = previous.kind; state.keys = previous.keys; throw error; }
}

export interface IChangeSetBinding<T> extends IDisposable {
  readonly Collection: ObservableCollection<T>;
  readonly Errors: Observable<unknown>;
  readonly IsDisposed: boolean;
  unsubscribe(): void;
}

/** One batch becomes one collection Edit. Disposing releases only this binding and its owned target. */
export function BindChangeSet<T, K = unknown>(source: DynamicDataSource<T, K>, target?: ObservableCollection<T>): IChangeSetBinding<T> {
  const collection = target ?? new ObservableCollection<T>();
  const errors = new ReplaySubject<unknown>(1), scope = new Subscription();
  let disposed = false, first = true, processing = false, stopRequested = false, completionRequested = false;
  const pending: DynamicChangeSetType<T, K>[] = [];
  // Each binding owns a fresh keyed model; reconcile it to an existing target on first delivery.
  let model: ObservableCollection<T> | undefined = new ObservableCollection<T>();
  const finish = () => { if (disposed) return; if (processing) { stopRequested = true; return; } disposed = true; pending.length = 0; scope.unsubscribe(); model?.Dispose(); model = undefined; if (!target) collection.Dispose(); errors.complete(); };
  const stream = new Observable<DynamicChangeSetType<T, K>>(observer => ConnectDynamicData(source).subscribe(observer));
  const observer = new Subscriber<DynamicChangeSetType<T, K>>({
    next(changes) {
      if (disposed || stopRequested || completionRequested) return;
      pending.push(changes); if (processing) return; processing = true;
      try {
        while (pending.length && !stopRequested) {
        const changes = pending.shift()!;
        if (first) {
          model!.ApplyChanges(changes);
          states.delete(collection);
          const snapshot = new DynamicChangeSet<T, K>([], changes.kind, model!.Items);
          if (changes.kind === 'cache') snapshot.keys = [...(states.get(model!)?.keys ?? [])] as K[];
          collection.ApplyChanges(snapshot); model!.Dispose(); model = undefined; first = false;
        } else collection.ApplyChanges(changes);
        }
      } catch (error) { errors.next(error); stopRequested = true; }
      finally { processing = false; if (stopRequested || completionRequested) finish(); }
    },
    error(error: unknown) { errors.next(error); if (processing) completionRequested = true; else finish(); },
    complete() { if (processing) completionRequested = true; else finish(); },
  });
  scope.add(observer); stream.subscribe(observer);
  return { Collection: collection, Errors: errors.asObservable(), get IsDisposed() { return disposed; },
    Dispose: finish,
    unsubscribe() { this.Dispose(); },
  };
}
export const ToReactiveCollection = BindChangeSet;
