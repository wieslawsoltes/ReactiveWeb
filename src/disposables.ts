/** A .NET-style resource lifetime. All built-in disposables also support RxJS unsubscribe. */
export interface IDisposable { Dispose(): void }
export type DisposableLike = IDisposable | { unsubscribe(): void } | (() => void) | null | undefined;

/** Dispose either a .NET-style resource, an RxJS subscription, or a teardown callback. */
export function dispose(resource: DisposableLike): void {
  if (!resource) return;
  if (typeof resource === 'function') resource();
  else if ('Dispose' in resource) resource.Dispose();
  else resource.unsubscribe();
}

function disposeAll(resources: Iterable<DisposableLike>): void {
  const errors: unknown[] = [];
  for (const resource of resources) { try { dispose(resource); } catch (error) { errors.push(error); } }
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) throw new AggregateError(errors, 'Multiple resources failed to dispose.');
}

export class Disposable implements IDisposable {
  private action: (() => void) | undefined;
  private disposed = false;
  constructor(action: () => void = () => {}) { this.action = action; }
  static Create(action: () => void): Disposable { return new Disposable(action); }
  static create(action: () => void): Disposable { return Disposable.Create(action); }
  static readonly Empty = Object.freeze({ Dispose() {}, unsubscribe() {}, IsDisposed: false, closed: false });
  get IsDisposed(): boolean { return this.disposed; }
  get closed(): boolean { return this.disposed; }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    const action = this.action;
    this.action = undefined;
    action?.();
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}

export class CompositeDisposable implements IDisposable, Iterable<DisposableLike> {
  private items: DisposableLike[] = [];
  private disposed = false;
  constructor(...resources: (DisposableLike | readonly DisposableLike[])[]) {
    for (const item of resources) {
      if (Array.isArray(item)) this.items.push(...item);
      else if (item) this.items.push(item as DisposableLike);
    }
  }
  get Count(): number { return this.items.length; }
  get IsDisposed(): boolean { return this.disposed; }
  get closed(): boolean { return this.disposed; }
  Add<T extends DisposableLike>(resource: T): T {
    if (this.disposed) dispose(resource);
    else if (resource) this.items.push(resource);
    return resource;
  }
  add<T extends DisposableLike>(resource: T): T { return this.Add(resource); }
  Remove(resource: DisposableLike): boolean {
    const index = this.items.indexOf(resource);
    if (index < 0) return false;
    this.items.splice(index, 1);
    dispose(resource);
    return true;
  }
  remove(resource: DisposableLike): boolean { return this.Remove(resource); }
  Contains(resource: DisposableLike): boolean { return this.items.includes(resource); }
  Clear(): void {
    const previous = this.items;
    this.items = [];
    disposeAll(previous);
  }
  clear(): void { this.Clear(); }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.Clear();
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
  [Symbol.iterator](): Iterator<DisposableLike> { return this.items.slice()[Symbol.iterator](); }
}

export class SerialDisposable implements IDisposable {
  private current: DisposableLike;
  private disposed = false;
  get Disposable(): DisposableLike { return this.current; }
  set Disposable(value: DisposableLike) {
    if (this.disposed) { dispose(value); return; }
    const old = this.current;
    this.current = value;
    dispose(old);
  }
  get disposable(): DisposableLike { return this.Disposable; }
  set disposable(value: DisposableLike) { this.Disposable = value; }
  get IsDisposed(): boolean { return this.disposed; }
  get closed(): boolean { return this.disposed; }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    const old = this.current;
    this.current = undefined;
    dispose(old);
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}

export class SingleAssignmentDisposable implements IDisposable {
  private current: DisposableLike;
  private assigned = false;
  private disposed = false;
  get Disposable(): DisposableLike { return this.current; }
  set Disposable(value: DisposableLike) {
    if (this.assigned) throw new Error('Disposable has already been assigned.');
    this.assigned = true;
    if (this.disposed) dispose(value);
    else this.current = value;
  }
  get disposable(): DisposableLike { return this.Disposable; }
  set disposable(value: DisposableLike) { this.Disposable = value; }
  get IsDisposed(): boolean { return this.disposed; }
  get closed(): boolean { return this.disposed; }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    const old = this.current;
    this.current = undefined;
    dispose(old);
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}

/** Releases the underlying resource once its owner and every acquired lease are disposed. */
export class RefCountDisposable implements IDisposable {
  private ownerDisposed = false;
  private disposed = false;
  private count = 0;
  constructor(private resource: DisposableLike) {}
  get IsDisposed(): boolean { return this.disposed; }
  get closed(): boolean { return this.disposed; }
  GetDisposable(): IDisposable & { unsubscribe(): void } {
    if (this.disposed) return Disposable.Empty;
    this.count++;
    return Disposable.Create(() => { this.count--; this.tryDispose(); });
  }
  getDisposable(): IDisposable & { unsubscribe(): void } { return this.GetDisposable(); }
  private tryDispose(): void {
    if (!this.disposed && this.ownerDisposed && this.count === 0) {
      this.disposed = true;
      const resource = this.resource;
      this.resource = undefined;
      dispose(resource);
    }
  }
  Dispose(): void { this.ownerDisposed = true; this.tryDispose(); }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}

export function DisposeWith<T extends DisposableLike>(resource: T, target: { Add(resource: DisposableLike): unknown } | { add(resource: DisposableLike): unknown }): T {
  if ('Add' in target) target.Add(resource);
  else target.add(resource);
  return resource;
}
export const disposeWith = DisposeWith;
