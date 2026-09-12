import { Observable, distinctUntilChanged } from 'rxjs';
import { WhenAnyValue } from './reactive-object.js';
import { CompositeDisposable, Disposable, SerialDisposable, dispose, type DisposableLike, type IDisposable } from './disposables.js';
import { ViewModelActivator, WhenActivated } from './activation.js';
import { ViewLocator } from './services.js';
import type { ValidationContext, IValidatableViewModel } from './validation.js';
import type { Interaction, InteractionHandler } from './interaction.js';
import { ConverterService, PropertyBindingHookRegistry, type BindingTypeToken } from './converters.js';
import { ToReactiveCollection, type DynamicDataSource } from './dynamic-data.js';
import type { ChangeSet } from './collections.js';

export interface IViewFor<T = unknown> { ViewModel: T | null }
export interface IBindingTypeConverter<TFrom = unknown, TTo = unknown> { Convert(value: TFrom): TTo; ConvertBack?(value: TTo): TFrom }
export interface BindingOptions<T = unknown, U = unknown> {
  event?: string;
  converter?: IBindingTypeConverter<T, U>;
  convert?: (value: T) => U;
  convertBack?: (value: U) => T;
  onError?: (error: unknown) => void;
  conversionService?: ConverterService;
  sourceType?: BindingTypeToken<T>;
  targetType?: BindingTypeToken<U>;
  conversionHint?: unknown;
  bindingHooks?: PropertyBindingHookRegistry;
}
export interface CommandLike<T = unknown, R = unknown> {
  readonly CanExecute: Observable<boolean>;
  readonly CanExecuteValue?: boolean;
  readonly IsExecuting?: Observable<boolean>;
  Execute(parameter: T): Observable<R>;
}
export class ReactiveBinding extends CompositeDisposable {
  get IsBound(): boolean { return !this.IsDisposed; }
}
/** Structural collection contract shared by HTML and React; snapshots are immutable. */
export interface ReactiveCollectionSource<T> {
  readonly Items: readonly T[];
  readonly ItemsChanged: Observable<readonly T[]>;
  readonly CollectionChanged?: Observable<ChangeSet<T>>;
  Connect?(): Observable<ChangeSet<T>>;
}
export type CollectionViewSource<T, K = unknown> = ReactiveCollectionSource<T> | DynamicDataSource<T, K>;
export interface CollectionBindingOptions<T> {
  /** Stable business identity. Repeated keys are matched by occurrence. */
  keySelector?: (item: T) => unknown;
  /** Updates retained nodes after refresh, replacement, or a changed index. */
  update?: (node: Node, item: T, index: number, lifetime: CompositeDisposable) => void;
  onError?: (error: unknown) => void;
}
export type CollectionItemRenderer<T> = (item: T, index: number, lifetime: CompositeDisposable) => Node;
interface CollectionRow<T> { item: T; key: unknown; node: Node; lifetime: CompositeDisposable }

/** Owns rendered rows and subscriptions, while the caller retains collection ownership. */
export class ReactiveCollectionBinding<T, K = unknown> extends ReactiveBinding {
  private source: CollectionViewSource<T, K>;
  private readonly connection = new SerialDisposable();
  private rows: CollectionRow<T>[] = [];
  private readonly ownedRows = new Set<CollectionRow<T>>();
  private readonly ownedNodes = new Set<Node>();
  private readonly anchor: Comment;
  private processing = false;
  private generation = 0;
  private readonly pending: (() => void)[] = [];
  constructor(source: CollectionViewSource<T, K>, readonly Element: Element, private readonly render: CollectionItemRenderer<T>, private readonly options: CollectionBindingOptions<T> = {}) {
    super(); this.source = source;
    this.anchor = Element.ownerDocument.createComment('ReactiveWeb collection'); Element.append(this.anchor);
    this.Add(this.connection);
    this.Add(() => { try { this.clearRows(); } finally { this.anchor.remove(); } });
    try { this.connect(); } catch (error) { this.Dispose(); throw error; }
  }
  get Source(): CollectionViewSource<T, K> { return this.source; }
  set Source(value: CollectionViewSource<T, K>) {
    if (this.IsDisposed) throw new Error('Collection binding is disposed');
    if (this.source === value) return;
    this.run(() => { this.connection.Disposable = undefined; this.clearRows(); this.source = value; this.connect(); });
  }
  private run(action: () => void): void {
    this.pending.push(action);
    if (this.processing) return;
    this.processing = true;
    try { while (this.pending.length && !this.IsDisposed) this.pending.shift()!(); }
    catch (error) { this.pending.length = 0; throw error; }
    finally { this.processing = false; if (this.IsDisposed) this.pending.length = 0; }
  }
  private clearRows(): void {
    const old = [...this.ownedRows]; this.rows = []; this.ownedRows.clear(); this.ownedNodes.clear();
    const resources = new CompositeDisposable(...old.map(row => () => { try { row.lifetime.Dispose(); } finally { row.node.parentNode?.removeChild(row.node); } }));
    resources.Dispose();
  }
  private createRow(item: T, index: number): CollectionRow<T> {
    const lifetime = new CompositeDisposable();
    try {
      const node = this.render(item, index, lifetime);
      if (!node || typeof node.nodeType !== 'number' || node.nodeType === 11) throw new TypeError('Collection renderer must return one persistent DOM node, not a DocumentFragment');
      if (this.ownedNodes.has(node)) throw new TypeError('Collection renderer must return a distinct node for each occurrence');
      const row = { item, key: this.options.keySelector ? this.options.keySelector(item) : item, node, lifetime };
      this.ownedRows.add(row); this.ownedNodes.add(node); return row;
    } catch (error) { lifetime.Dispose(); throw error; }
  }
  private removeRows(rows: CollectionRow<T>[]): void {
    for (const row of rows) { this.ownedRows.delete(row); this.ownedNodes.delete(row.node); }
    new CompositeDisposable(...rows.map(row => () => { try { row.lifetime.Dispose(); } finally { row.node.parentNode?.removeChild(row.node); } })).Dispose();
  }
  private reconcile(items: readonly T[]): void {
    const available = new Map<unknown, CollectionRow<T>[]>();
    for (const row of this.rows) { const bucket = available.get(row.key) ?? []; bucket.push(row); available.set(row.key, bucket); }
    const next: CollectionRow<T>[] = [];
    for (let index = 0; index < items.length; index++) {
      const item = items[index]!;
      const key = this.options.keySelector ? this.options.keySelector(item) : item;
      const bucket = available.get(key);
      let row: CollectionRow<T> | undefined;
      if (bucket?.length) {
        const same = bucket.findIndex(candidate => Object.is(candidate.item, item));
        if (same >= 0) row = bucket.splice(same, 1)[0];
        else if (this.options.update) row = bucket.shift();
      }
      if (!row) row = this.createRow(item, index);
      row.item = item; next.push(row);
    }
    this.rows = next;
    this.removeRows([...available.values()].flat());
    this.layout();
  }
  private apply(changes: ChangeSet<T>): void {
    for (const change of changes) {
      if (change.Reason === 'reset') { this.reconcile(change.Items); continue; }
      if (change.Reason === 'add') this.rows.splice(change.Index, 0, ...change.Items.map((item, index) => this.createRow(item, change.Index + index)));
      else if (change.Reason === 'remove') this.removeRows(this.rows.splice(change.Index, change.Items.length));
      else if (change.Reason === 'move') this.rows.splice(change.Index, 0, ...this.rows.splice(change.PreviousIndex!, change.Items.length));
      else if (change.Reason === 'replace') {
        const old = this.rows.splice(change.Index, change.PreviousItems?.length ?? change.Items.length);
        const next = change.Items.map((item, index) => {
          const key = this.options.keySelector ? this.options.keySelector(item) : item;
          const match = old.findIndex(row => Object.is(row.key, key) && (Object.is(row.item, item) || !!this.options.update));
          const row = match < 0 ? this.createRow(item, change.Index + index) : old.splice(match, 1)[0]!;
          row.item = item; return row;
        });
        this.rows.splice(change.Index, 0, ...next); this.removeRows(old);
      }
    }
    this.layout();
  }
  private layout(): void {
    // Work backwards: insertBefore moves existing nodes without remounting them.
    let reference: Node = this.anchor;
    for (let index = this.rows.length - 1; index >= 0; index--) {
      const row = this.rows[index]!;
      if (row.node.nextSibling !== reference || row.node.parentNode !== this.Element) this.Element.insertBefore(row.node, reference);
      this.options.update?.(row.node, row.item, index, row.lifetime); reference = row.node;
    }
  }
  private connect(): void {
    const lifetime = new CompositeDisposable(); this.connection.Disposable = lifetime;
    const generation = ++this.generation;
    const source = this.source;
    const binding = 'ItemsChanged' in source ? undefined : ToReactiveCollection(source as DynamicDataSource<T, K>);
    if (binding) lifetime.Add(binding);
    const collection = (binding?.Collection ?? source) as ReactiveCollectionSource<T>;
    let subscribing = true, failed = false, initialError: unknown;
    const fail = (error: unknown) => {
      if (generation !== this.generation) return;
      lifetime.Dispose(); this.clearRows();
      if (this.options.onError) this.options.onError(error);
      else if (subscribing) { failed = true; initialError = error; }
      else throw error;
    };
    if (collection.Connect) {
      // The atomic initial reset excludes a batch already reflected in the current snapshot.
      lifetime.Add(collection.Connect().subscribe({ next: changes => { try { this.run(() => { if (generation === this.generation) this.apply(changes); }); } catch (error) { fail(error); } }, error: fail }));
    } else lifetime.Add(collection.ItemsChanged.subscribe({ next: items => { try { this.run(() => { if (generation === this.generation) this.reconcile(items); }); } catch (error) { fail(error); } }, error: fail }));
    if (binding) lifetime.Add(binding.Errors.subscribe(fail));
    subscribing = false;
    if (failed) throw initialError;
  }
}
/** Binds DynamicData list/cache changes or a bindable collection to independently owned rows. */
export function BindCollection<T, K = unknown>(source: CollectionViewSource<T, K>, element: Element, render: CollectionItemRenderer<T>, options: CollectionBindingOptions<T> = {}): ReactiveCollectionBinding<T, K> {
  return new ReactiveCollectionBinding(source, element, render, options);
}
export const bindCollection = BindCollection;

const invalidParts = new Set(['__proto__', 'prototype', 'constructor']);
function pathParts(path: string): string[] {
  const parts = path.split('.');
  if (!parts.length || parts.some(part => !/^[A-Za-z_$][\w$]*$/.test(part) || invalidParts.has(part))) throw new TypeError(`Unsafe or invalid binding path: ${path}`);
  return parts;
}
function readPath(source: unknown, path: string): unknown {
  return pathParts(path).reduce<unknown>((value, key) => value == null ? undefined : (value as Record<string, unknown>)[key], source);
}
function writePath(source: unknown, path: string, value: unknown): void {
  const parts = pathParts(path);
  const last = parts.pop()!;
  const target = parts.reduce<unknown>((current, key) => current == null ? undefined : (current as Record<string, unknown>)[key], source);
  if (target == null || (typeof target !== 'object' && typeof target !== 'function')) throw new TypeError(`Cannot write binding path ${path}: its parent is null.`);
  (target as Record<string, unknown>)[last] = value;
}
function bindingError(target: Element, error: unknown, handler?: (error: unknown) => void): void {
  if (handler) { handler(error); return; }
  const EventType = target.ownerDocument.defaultView?.CustomEvent ?? globalThis.CustomEvent;
  if (EventType) target.dispatchEvent(new EventType('reactive-error', { detail: error, bubbles: true, composed: true }));
}
function defaultProperty(element: Element): string {
  return element.localName === 'input' && ['checkbox', 'radio'].includes(element.getAttribute('type') ?? '') ? 'checked' : ['input', 'textarea', 'select'].includes(element.localName) ? 'value' : 'textContent';
}
function applyValue(element: Element, property: string, value: unknown): void {
  if (property.startsWith('attr.')) {
    const attribute = property.slice(5);
    if (/^on/i.test(attribute) || ['srcdoc', 'style'].includes(attribute.toLowerCase())) throw new TypeError('Executable attributes are not binding targets.');
    if (['href', 'src', 'action', 'formaction', 'xlink:href'].includes(attribute.toLowerCase()) && /^\s*(?:javascript|vbscript|data):/i.test(String(value))) throw new TypeError('Unsafe binding URL.');
    if (value == null || value === false) element.removeAttribute(attribute);
    else element.setAttribute(attribute, value === true ? '' : String(value));
  } else if (property.startsWith('class.')) {
    element.classList.toggle(property.slice(6), Boolean(value));
  } else {
    pathParts(property);
    if (['innerHTML', 'outerHTML', 'srcdoc'].includes(property) || /^on/i.test(property)) throw new TypeError('Use trusted DOM construction for HTML and event handlers.');
    const target = element as unknown as Record<string, unknown>;
    const next = ['textContent', 'value'].includes(property) ? value ?? '' : value;
    if (!Object.is(target[property], next)) target[property] = next;
  }
}

/** Binds an observable directly to a DOM property and returns its subscription lifetime. */
export function BindTo<T>(source: Observable<T>, element: Element, property = defaultProperty(element), options: BindingOptions<T, any> = {}): ReactiveBinding {
  const binding = new ReactiveBinding();
  binding.Add(source.pipe(distinctUntilChanged()).subscribe({
    next: value => { try { applyValue(element, property, options.convert ? options.convert(value) : options.converter ? options.converter.Convert(value) : options.targetType ? (options.conversionService ?? ConverterService.Current).Convert(value, options.targetType, options.conversionHint, options.sourceType) : value); } catch (error) { bindingError(element, error, options.onError); } },
    error: error => bindingError(element, error, options.onError),
  }));
  return binding;
}
export function OneWayBind<T extends object>(viewModel: T, path: string, element: Element, property = defaultProperty(element), options: BindingOptions<any, any> = {}): ReactiveBinding {
  pathParts(path);
  if (!(options.bindingHooks ?? PropertyBindingHookRegistry.Current).ExecuteHooks({ ViewModel: viewModel, View: element, SourceProperty: path, TargetProperty: property, Direction: 'OneWay' })) { const rejected = new ReactiveBinding(); rejected.Dispose(); return rejected; }
  return BindTo(WhenAnyValue(viewModel, path), element, property, options);
}
/** Two-way binding; inputs use input, select/checkbox/radio use change by default. */
export function Bind<T extends object>(viewModel: T, path: string, element: Element, property = defaultProperty(element), options: BindingOptions<any, any> = {}): ReactiveBinding {
  pathParts(path);
  if (!(options.bindingHooks ?? PropertyBindingHookRegistry.Current).ExecuteHooks({ ViewModel: viewModel, View: element, SourceProperty: path, TargetProperty: property, Direction: 'TwoWay' })) { const rejected = new ReactiveBinding(); rejected.Dispose(); return rejected; }
  const binding = BindTo(WhenAnyValue(viewModel, path), element, property, options);
  const eventName = options.event ?? (element.localName === 'select' || property === 'checked' ? 'change' : 'input');
  let composing = false;
  const change = () => {
    if (composing) return;
    try {
      const value = (element as unknown as Record<string, unknown>)[property];
      writePath(viewModel, path, options.convertBack ? options.convertBack(value) : options.converter?.ConvertBack ? options.converter.ConvertBack(value) : options.sourceType ? (options.conversionService ?? ConverterService.Current).Convert(value, options.sourceType, options.conversionHint, options.targetType) : value);
    } catch (error) { bindingError(element, error, options.onError); }
  };
  const start = () => { composing = true; };
  const end = () => { composing = false; change(); };
  element.addEventListener(eventName, change);
  element.addEventListener('compositionstart', start);
  element.addEventListener('compositionend', end);
  binding.Add(() => { element.removeEventListener(eventName, change); element.removeEventListener('compositionstart', start); element.removeEventListener('compositionend', end); });
  return binding;
}
export interface CommandBindingOptions<T> { event?: string; parameter?: T | ((event: Event) => T); preventDefault?: boolean; onError?: (error: unknown) => void }
export function BindCommand<T, R>(command: CommandLike<T, R>, element: Element, options: CommandBindingOptions<T> = {}): ReactiveBinding {
  const binding = new ReactiveBinding();
  let canExecute = false;
  const hadAria = element.getAttribute('aria-disabled');
  const originalDisabled = (element as HTMLButtonElement).disabled;
  binding.Add(command.CanExecute.subscribe(value => { canExecute = value; if ('disabled' in element) (element as HTMLButtonElement).disabled = !value; element.setAttribute('aria-disabled', String(!value)); }));
  const execute = (event: Event) => {
    if (options.preventDefault ?? true) event.preventDefault();
    if (!canExecute || command.CanExecuteValue === false) return;
    try {
      const parameter = typeof options.parameter === 'function' ? (options.parameter as (event: Event) => T)(event) : options.parameter as T;
      // A completed execution is removed immediately; long-lived views do not accumulate subscriptions.
      const execution = command.Execute(parameter).subscribe({ error: error => bindingError(element, error, options.onError) });
      binding.Add(execution);
      execution.add(() => binding.Remove(execution));
    } catch (error) { bindingError(element, error, options.onError); }
  };
  const eventName = options.event ?? 'click';
  element.addEventListener(eventName, execute);
  binding.Add(() => { element.removeEventListener(eventName, execute); if ('disabled' in element) (element as HTMLButtonElement).disabled = originalDisabled; if (hadAria == null) element.removeAttribute('aria-disabled'); else element.setAttribute('aria-disabled', hadAria); });
  return binding;
}
export function BindValidation(viewModel: IValidatableViewModel | ValidationContext, element: Element, propertyName?: string, separator = '\n'): ReactiveBinding {
  const context = 'ValidationContext' in viewModel ? viewModel.ValidationContext : viewModel;
  const binding = new ReactiveBinding();
  binding.Add(context.ObserveErrors(propertyName).subscribe(errors => { element.textContent = errors.join(separator); element.setAttribute('aria-live', 'polite'); (element as HTMLElement).hidden = errors.length === 0; }));
  return binding;
}

/** Re-registers the handler if a reactive interaction property is replaced. */
export function BindInteraction<TInput, TOutput>(viewModel: object, path: string, handler: InteractionHandler<TInput, TOutput>): ReactiveBinding {
  pathParts(path);
  const binding = new ReactiveBinding();
  const registration = binding.Add(new SerialDisposable());
  binding.Add(WhenAnyValue(viewModel, path).subscribe(interaction => {
    registration.Disposable = undefined;
    if (interaction) registration.Disposable = (interaction as Interaction<TInput, TOutput>).RegisterHandler(handler);
  }));
  return binding;
}

export const BindingConverters = Object.freeze({
  String: { Convert: (value: unknown) => String(value ?? ''), ConvertBack: (value: string) => value },
  Number: { Convert: (value: number | null) => value == null ? '' : String(value), ConvertBack: (value: string) => { if (value.trim() === '') return null; const number = Number(value); if (!Number.isFinite(number)) throw new TypeError('Enter a finite number.'); return number; } },
  Boolean: { Convert: (value: unknown) => Boolean(value), ConvertBack: (value: unknown) => Boolean(value) },
  Not: { Convert: (value: unknown) => !value, ConvertBack: (value: unknown) => !value },
});
export interface HtmlBindingOptions { converters?: Record<string, IBindingTypeConverter<any, any>>; observeMutations?: boolean; onError?: (error: unknown) => void }
const selector = '[data-rx-text],[data-rx-bind],[data-rx-value],[data-rx-checked],[data-rx-visible],[data-rx-enabled],[data-rx-command],[data-rx-validation],[data-rx-one-way]';

/** Declarative bindings use property paths only, never eval or expression compilation. */
export function BindHtml(root: ParentNode, viewModel: object, options: HtmlBindingOptions = {}): ReactiveBinding {
  const result = new ReactiveBinding();
  const bound = new Map<Element, ReactiveBinding>();
  const bindElement = (element: Element) => {
    if (bound.has(element)) return;
    const binding = new ReactiveBinding();
    bound.set(element, binding);
    try {
      const converterName = element.getAttribute('data-rx-converter');
      const converter = converterName ? options.converters?.[converterName] ?? (BindingConverters as Record<string, IBindingTypeConverter<any, any>>)[converterName] : undefined;
      if (converterName && !converter) throw new TypeError(`Unknown binding converter: ${converterName}`);
      const bindingOptions = { converter, onError: options.onError, event: element.getAttribute('data-rx-event') ?? undefined };
      for (const attribute of ['data-rx-bind', 'data-rx-value', 'data-rx-checked']) {
        const path = element.getAttribute(attribute);
        if (path) binding.Add(Bind(viewModel, path, element, attribute === 'data-rx-checked' ? 'checked' : defaultProperty(element), bindingOptions));
      }
      for (const [attribute, property, convert] of [
        ['data-rx-text', 'textContent', undefined], ['data-rx-one-way', defaultProperty(element), undefined],
        ['data-rx-visible', 'hidden', (value: unknown) => !value], ['data-rx-enabled', 'disabled', (value: unknown) => !value],
      ] as const) {
        const path = element.getAttribute(attribute);
        if (path) binding.Add(OneWayBind(viewModel, path, element, property, { ...bindingOptions, convert }));
      }
      const commandPath = element.getAttribute('data-rx-command');
      if (commandPath) {
        pathParts(commandPath);
        const current = new SerialDisposable();
        binding.Add(current);
        binding.Add(WhenAnyValue(viewModel, commandPath).subscribe(value => {
          current.Disposable = undefined;
          if (!value) { if ('disabled' in element) (element as HTMLButtonElement).disabled = true; return; }
          current.Disposable = BindCommand(value as CommandLike, element, { event: bindingOptions.event, onError: options.onError, parameter: () => {
            const parameterPath = element.getAttribute('data-rx-parameter');
            return parameterPath ? readPath(viewModel, parameterPath) : undefined;
          } });
        }));
      }
      if (element.hasAttribute('data-rx-validation')) {
        if (!('ValidationContext' in viewModel)) throw new TypeError('Validation binding requires a ValidationContext.');
        binding.Add(BindValidation(viewModel as IValidatableViewModel, element, element.getAttribute('data-rx-validation') || undefined));
      }
    } catch (error) { binding.Dispose(); bound.delete(element); bindingError(element, error, options.onError); }
  };
  const ownsElement = (element: Element): boolean => {
    let ancestor: Element | null = element;
    while (ancestor && ancestor !== root) {
      if ('ViewModel' in ancestor || ancestor.hasAttribute('data-rx-scope')) return false;
      ancestor = ancestor.parentElement;
    }
    return true;
  };
  const reconcile = () => {
    const elements = new Set<Element>(Array.from(root.querySelectorAll(selector)).filter(ownsElement));
    if ('matches' in root && (root as Element).matches(selector)) elements.add(root as Element);
    for (const [element, binding] of bound) if (!elements.has(element)) { binding.Dispose(); bound.delete(element); }
    for (const element of elements) bindElement(element);
  };
  reconcile();
  const document = 'ownerDocument' in root ? root.ownerDocument : root as Document;
  const Observer = document?.defaultView?.MutationObserver ?? globalThis.MutationObserver;
  if ((options.observeMutations ?? true) && Observer) {
    const observer = new Observer(mutations => {
      for (const mutation of mutations) if (mutation.type === 'attributes' && mutation.attributeName?.startsWith('data-rx-')) { const element = mutation.target as Element; bound.get(element)?.Dispose(); bound.delete(element); }
      reconcile();
    });
    observer.observe(root as Node, { childList: true, subtree: true, attributes: true, attributeFilter: ['data-rx-text', 'data-rx-bind', 'data-rx-value', 'data-rx-checked', 'data-rx-visible', 'data-rx-enabled', 'data-rx-command', 'data-rx-validation', 'data-rx-one-way', 'data-rx-converter', 'data-rx-event', 'data-rx-parameter', 'data-rx-scope'] });
    result.Add(() => observer.disconnect());
  }
  result.Add(() => { for (const binding of bound.values()) binding.Dispose(); bound.clear(); });
  return result;
}

// Safe to import during SSR. Construct/register elements only in a DOM environment.
const HTMLElementBase = (globalThis.HTMLElement ?? class {}) as typeof HTMLElement;
export class ReactiveElement<T extends object = object> extends HTMLElementBase implements IViewFor<T> {
  readonly Activator = new ViewModelActivator();
  private viewModel: T | null = null;
  private lifetime?: CompositeDisposable;
  private disposed = false;
  get ViewModel(): T | null { return this.viewModel; }
  set ViewModel(value: T | null) { if (this.viewModel === value) return; this.viewModel = value; this.RefreshBindings(); }
  get DataContext(): T | null { return this.ViewModel; }
  set DataContext(value: T | null) { this.ViewModel = value; }
  get BindingRoot(): ParentNode { return this.shadowRoot ?? this; }
  connectedCallback(): void { if (!this.disposed) this.activate(); }
  disconnectedCallback(): void { this.lifetime?.Dispose(); this.lifetime = undefined; }
  WhenActivated(block: (disposables: CompositeDisposable) => void | DisposableLike): IDisposable { return WhenActivated(this.Activator, block); }
  protected OnActivated(_disposables: CompositeDisposable): void {}
  protected RefreshBindings(): void { if (!this.isConnected || this.disposed) return; this.disconnectedCallback(); this.activate(); }
  private activate(): void {
    if (this.lifetime) return;
    const lifetime = this.lifetime = new CompositeDisposable();
    try {
      if (this.ViewModel) {
        const activator = (this.ViewModel as { Activator?: { Activate(): IDisposable } }).Activator;
        if (activator) lifetime.Add(activator.Activate());
        lifetime.Add(BindHtml(this.BindingRoot, this.ViewModel));
      }
      lifetime.Add(this.Activator.Activate());
      this.OnActivated(lifetime);
    } catch (error) { lifetime.Dispose(); this.lifetime = undefined; throw error; }
  }
  Dispose(): void { if (this.disposed) return; this.disposed = true; this.disconnectedCallback(); this.Activator.Dispose(); }
  unsubscribe(): void { this.Dispose(); }
}
export { ReactiveElement as ReactiveUserControl };

export interface HtmlView extends IViewFor { readonly Element: Node; Dispose?(): void }
export interface ViewResolver { ResolveView(viewModel: unknown, contract?: string): unknown; IsSingletonView?(view: unknown): boolean }
function mountView(host: Element, viewModel: unknown, locator: ViewResolver, contract: string | undefined, fallback: Node | string | null): IDisposable {
  const resolved = viewModel == null ? undefined : locator.ResolveView(viewModel, contract);
  let node: Node | undefined;
  if (resolved && typeof resolved === 'object') {
    if ('ViewModel' in resolved) (resolved as IViewFor).ViewModel = viewModel;
    if ('DataContext' in resolved) (resolved as { DataContext: unknown }).DataContext = viewModel;
    if ('nodeType' in resolved) node = resolved as Node;
    else if ('Element' in resolved) node = (resolved as HtmlView).Element;
  }
  if (resolved != null && !node) throw new TypeError('An HTML view factory must return a DOM Node or { Element, ViewModel }.');
  if (node) host.replaceChildren(node);
  else if (typeof fallback === 'string') host.textContent = fallback;
  else if (fallback) host.replaceChildren(fallback);
  else host.replaceChildren();
  return Disposable.Create(() => { host.replaceChildren(); if (resolved && typeof resolved === 'object' && 'Dispose' in resolved && !locator.IsSingletonView?.(resolved)) dispose(resolved as IDisposable); });
}
export class ViewModelViewHost extends ReactiveElement {
  private locator: ViewResolver = ViewLocator.Current;
  get ViewLocator(): ViewResolver { return this.locator; }
  set ViewLocator(value: ViewResolver) { if (this.locator !== value) { this.locator = value; this.RefreshBindings(); } }
  private contract?: string;
  private fallback: Node | string | null = null;
  get ViewContract(): string | undefined { return this.contract; }
  set ViewContract(value: string | undefined) { if (this.contract !== value) { this.contract = value; this.RefreshBindings(); } }
  get DefaultContent(): Node | string | null { return this.fallback; }
  set DefaultContent(value: Node | string | null) { this.fallback = value; this.RefreshBindings(); }
  protected override OnActivated(disposables: CompositeDisposable): void { disposables.Add(mountView(this, this.ViewModel, this.ViewLocator, this.ViewContract, this.DefaultContent)); }
}
export interface RouterLike { readonly CurrentViewModel: Observable<unknown>; readonly CurrentViewModelValue?: unknown }
export class RoutedViewHost extends ReactiveElement {
  private locator: ViewResolver = ViewLocator.Current;
  get ViewLocator(): ViewResolver { return this.locator; }
  set ViewLocator(value: ViewResolver) { if (this.locator !== value) { this.locator = value; this.RefreshBindings(); } }
  private router: RouterLike | null = null;
  private contract?: string;
  private fallback: Node | string | null = null;
  get Router(): RouterLike | null { return this.router; }
  set Router(value: RouterLike | null) { if (this.router !== value) { this.router = value; this.RefreshBindings(); } }
  get ViewContract(): string | undefined { return this.contract; }
  set ViewContract(value: string | undefined) { if (this.contract !== value) { this.contract = value; this.RefreshBindings(); } }
  get DefaultContent(): Node | string | null { return this.fallback; }
  set DefaultContent(value: Node | string | null) { this.fallback = value; this.RefreshBindings(); }
  protected override OnActivated(disposables: CompositeDisposable): void {
    const view = disposables.Add(new SerialDisposable());
    const render = (value: unknown) => { view.Disposable = undefined; view.Disposable = mountView(this, value, this.ViewLocator, this.ViewContract, this.DefaultContent); };
    if (this.Router) disposables.Add(this.Router.CurrentViewModel.subscribe(render));
    else render(null);
  }
}
/** Explicit registration avoids globals and allows multiple versions in the same document. */
export function RegisterReactiveElements(registry: CustomElementRegistry = globalThis.customElements, prefix = 'reactive'): void {
  if (!registry) throw new Error('A CustomElementRegistry is required.');
  for (const [name, type] of [[`${prefix}-view`, ReactiveElement], [`${prefix}-view-host`, ViewModelViewHost], [`${prefix}-routed-view-host`, RoutedViewHost]] as const) {
    const Base = type as CustomElementConstructor;
    if (!registry.get(name)) registry.define(name, class extends Base {});
  }
}
export const bind = Bind;
export const oneWayBind = OneWayBind;
export const bindTo = BindTo;
export const bindCommand = BindCommand;
export const bindHtml = BindHtml;
