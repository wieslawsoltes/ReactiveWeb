import { createContext, createElement, useCallback, useContext, useEffect, useMemo, useRef, useSyncExternalStore, type ComponentType, type DependencyList, type ReactElement, type ReactNode } from 'react';
import { Observable, Subscription, firstValueFrom, switchMap, take } from 'rxjs';
import { CompositeDisposable, dispose, type DisposableLike, type IDisposable } from './disposables.js';
import type { ReactiveObject } from './reactive-object.js';
import type { CommandLike, RouterLike, ViewResolver, CollectionViewSource, ReactiveCollectionSource } from './html.js';
import { ToReactiveCollection, type DynamicDataSource } from './dynamic-data.js';
import { ViewLocator } from './services.js';

interface ObservableStore<T> { subscribe(listener: () => void): () => void; getSnapshot(): T; getServerSnapshot(): T }
function makeObservableStore<T>(source: Observable<T>, initial: T, serverValue: T): ObservableStore<T> {
  let value = initial;
  let failure: unknown;
  let failed = false;
  let subscription: Subscription | undefined;
  const listeners = new Set<() => void>();
  const notify = () => { for (const listener of [...listeners]) listener(); };
  return {
    subscribe(listener) {
      listeners.add(listener);
      if (!subscription) {
        subscription = new Subscription();
        subscription.add(source.subscribe({
          next(next) { if (!Object.is(value, next) || failed) { value = next; failed = false; notify(); } },
          error(error) { failed = true; failure = error; notify(); },
        }));
      }
      return () => { listeners.delete(listener); if (!listeners.size) { subscription?.unsubscribe(); subscription = undefined; } };
    },
    getSnapshot() { if (failed) throw failure; return value; },
    getServerSnapshot() { return serverValue; },
  };
}

/** Subscribes after commit; caches snapshots and releases subscriptions on unmount. */
export function useObservable<T>(source: Observable<T>, initialValue: T, serverValue?: T): T;
export function useObservable<T>(source: Observable<T>): T | undefined;
export function useObservable<T>(source: Observable<T>, initialValue?: T, serverValue: T | undefined = initialValue): T | undefined {
  // BehaviorSubject values can be read without starting work during a render.
  const initial = initialValue === undefined && 'getValue' in source ? (source as Observable<T> & { getValue(): T }).getValue() : initialValue;
  const store = useMemo(() => makeObservableStore(source as Observable<T | undefined>, initial, serverValue === undefined ? initial : serverValue), [source]);
  return useSyncExternalStore(store.subscribe, store.getSnapshot, store.getServerSnapshot);
}

function makeCollectionStore<T, K>(source: CollectionViewSource<T, K>, serverSnapshot?: readonly T[]): ObservableStore<readonly T[]> {
  // Reading Items starts no observable work, including on the server or abandoned renders.
  const initial = 'Items' in source ? Array.from(source.Items) as T[] : [];
  let snapshot: readonly T[] = Object.freeze(initial);
  const server = serverSnapshot === undefined ? snapshot : Object.freeze([...serverSnapshot]);
  let connection: CompositeDisposable | undefined;
  let failure: unknown, failed = false;
  const listeners = new Set<() => void>();
  const notify = () => { for (const listener of [...listeners]) listener(); };
  return {
    subscribe(listener) {
      listeners.add(listener);
      if (!connection) {
        const lifetime = new CompositeDisposable(); connection = lifetime;
        const binding = 'ItemsChanged' in source ? undefined : ToReactiveCollection(source as DynamicDataSource<T, K>);
        if (binding) lifetime.Add(binding);
        const collection = (binding?.Collection ?? source) as ReactiveCollectionSource<T>;
        // A finite synchronous source may complete its snapshot subject before subscription.
        snapshot = Object.freeze([...collection.Items]); failed = false; notify();
        const error = (reason: unknown) => { failed = true; failure = reason; notify(); };
        lifetime.Add(collection.ItemsChanged.subscribe({
          next(items) { snapshot = Object.freeze([...items]); failed = false; notify(); }, error,
        }));
        if (binding) lifetime.Add(binding.Errors.subscribe(error));
      }
      return () => { listeners.delete(listener); if (!listeners.size) { const old = connection; connection = undefined; old?.Dispose(); } };
    },
    getSnapshot() { if (failed) throw failure; return snapshot; },
    getServerSnapshot() { return server; },
  };
}
/** Immutable, cached collection snapshots; subscribes only after commit and owns no source. */
export function useReactiveCollection<T, K = unknown>(source: CollectionViewSource<T, K>, serverSnapshot?: readonly T[]): readonly T[] {
  // A server snapshot initializes a source. Inline snapshot literals must not recreate subscriptions.
  const store = useMemo(() => makeCollectionStore(source, serverSnapshot), [source]);
  return useSyncExternalStore(store.subscribe, store.getSnapshot, store.getServerSnapshot);
}
export const useCollection = useReactiveCollection;

interface ReactiveStore { subscribe(listener: () => void): () => void; getSnapshot(): number; getServerSnapshot(): number }
const reactiveStores = new WeakMap<object, ReactiveStore>();
function reactiveStore(source: Pick<ReactiveObject, 'Changed'>): ReactiveStore {
  const existing = reactiveStores.get(source);
  if (existing) return existing;
  let revision = 0;
  let subscription: Subscription | undefined;
  const listeners = new Set<() => void>();
  const store: ReactiveStore = {
    subscribe(listener) {
      listeners.add(listener);
      if (!subscription) {
        // Re-check mutations between render and subscribe, including StrictMode remounts.
        revision++;
        subscription = source.Changed.subscribe(() => { revision++; for (const notify of [...listeners]) notify(); });
      }
      return () => { listeners.delete(listener); if (!listeners.size) { subscription?.unsubscribe(); subscription = undefined; } };
    },
    getSnapshot: () => revision,
    getServerSnapshot: () => 0,
  };
  reactiveStores.set(source, store);
  return store;
}
export function useReactiveObject<T extends Pick<ReactiveObject, 'Changed'>>(viewModel: T): T;
export function useReactiveObject<T extends Pick<ReactiveObject, 'Changed'>, R>(viewModel: T, selector: (viewModel: T) => R): R;
export function useReactiveObject<T extends Pick<ReactiveObject, 'Changed'>, R>(viewModel: T, selector?: (viewModel: T) => R): T | R {
  const store = useMemo(() => reactiveStore(viewModel), [viewModel]);
  useSyncExternalStore(store.subscribe, store.getSnapshot, store.getServerSnapshot);
  return selector ? selector(viewModel) : viewModel;
}

export interface Activatable { readonly Activator: { Activate(): IDisposable } }
/** React StrictMode setup-cleanup-setup owns one independent activation lease per setup. */
export function useWhenActivated(viewModel: Activatable, block?: (disposables: CompositeDisposable) => void | DisposableLike, dependencies: DependencyList = []): void {
  const blockRef = useRef(block);
  blockRef.current = block;
  useEffect(() => {
    const lifetime = new CompositeDisposable();
    try { lifetime.Add(viewModel.Activator.Activate()); const resource = blockRef.current?.(lifetime); if (resource) lifetime.Add(resource); }
    catch (error) { lifetime.Dispose(); throw error; }
    return () => lifetime.Dispose();
  }, [viewModel, ...dependencies]);
}
export interface ReactiveCommandHook<T, R> {
  readonly canExecute: boolean;
  readonly isExecuting: boolean;
  /** Resolves the command's first result; rejects failures and blocked executions. */
  readonly execute: (parameter: T) => Promise<R>;
  readonly CanExecute: boolean;
  readonly IsExecuting: boolean;
  readonly Execute: (parameter: T) => Promise<R>;
}
const notExecuting = new Observable<boolean>(subscriber => { subscriber.next(false); subscriber.complete(); });
export function useReactiveCommand<T, R>(command: CommandLike<T, R>): ReactiveCommandHook<T, R> {
  const canExecute = useObservable(command.CanExecute, false);
  const isExecuting = useObservable(command.IsExecuting ?? notExecuting, false);
  const execute = useCallback((parameter: T) => firstValueFrom(command.CanExecute.pipe(take(1), switchMap(enabled => {
    if (!enabled || ('CanExecuteValue' in command && command.CanExecuteValue === false)) throw new Error('The command cannot execute in its current state.');
    return command.Execute(parameter);
  }))), [command]);
  return useMemo(() => ({ canExecute, isExecuting, execute, CanExecute: canExecute, IsExecuting: isExecuting, Execute: execute }), [canExecute, isExecuting, execute]);
}

const ViewModelContext = createContext<unknown>(undefined);
export function ReactiveProvider<T>({ viewModel, children }: { viewModel: T; children?: ReactNode }): ReactElement {
  return createElement(ViewModelContext.Provider, { value: viewModel }, children);
}
export function useViewModel<T>(): T {
  const viewModel = useContext(ViewModelContext);
  if (viewModel === undefined) throw new Error('useViewModel must be used inside ReactiveProvider.');
  return viewModel as T;
}
export function createReactiveContext<T>() {
  const context = createContext<T | undefined>(undefined);
  const Provider = ({ viewModel, children }: { viewModel: T; children?: ReactNode }) => createElement(context.Provider, { value: viewModel }, children);
  const useReactiveViewModel = (): T => {
    const value = useContext(context);
    if (value === undefined) throw new Error('Reactive context provider is missing.');
    return value;
  };
  return { Provider, useViewModel: useReactiveViewModel, Context: context };
}

export interface ViewModelViewHostProps<T = unknown> { viewModel: T | null; viewLocator?: ViewResolver; contract?: string; fallback?: ReactNode }
/** Register a factory returning a React component type, e.g. () => DetailsView. */
export function ViewModelViewHost<T>({ viewModel, viewLocator = ViewLocator.Current, contract, fallback = null }: ViewModelViewHostProps<T>): ReactNode {
  const resolved = useMemo(() => viewModel == null ? undefined : viewLocator.ResolveView(viewModel, contract), [viewModel, viewLocator, contract]);
  // Instances with explicit resource ownership are released when a view is replaced.
  useEffect(() => () => { if (resolved && typeof resolved === 'object' && 'Dispose' in resolved) dispose(resolved as IDisposable); }, [resolved]);
  if (!resolved || viewModel == null) return fallback;
  return createElement(ReactiveProvider, { viewModel }, createElement(resolved as ComponentType<{ viewModel: T; ViewModel: T }>, { viewModel, ViewModel: viewModel }));
}
export interface RoutedViewHostProps extends Omit<ViewModelViewHostProps, 'viewModel'> { router: RouterLike; initialViewModel?: unknown }
export function RoutedViewHost({ router, initialViewModel, ...props }: RoutedViewHostProps): ReactNode {
  const viewModel = useObservable(router.CurrentViewModel, initialViewModel ?? router.CurrentViewModelValue ?? null);
  return createElement(ViewModelViewHost, { ...props, viewModel });
}
