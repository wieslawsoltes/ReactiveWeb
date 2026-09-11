import { BehaviorSubject, Observable, Subject } from 'rxjs';
import { CompositeDisposable, Disposable, type IDisposable, type DisposableLike } from './disposables.js';

export type ActivationBlock = (disposables: CompositeDisposable) => void | DisposableLike;
export interface IActivatableViewModel { readonly Activator: ViewModelActivator; }

/** Reference-counted activation. Every final release disposes the current activation scope. */
export class ViewModelActivator implements IDisposable {
  private readonly blocks = new Map<symbol, ActivationBlock>();
  private readonly activated = new Subject<void>();
  private readonly deactivated = new Subject<void>();
  private readonly active = new BehaviorSubject(false);
  private references = 0;
  private generation = 0;
  private scope?: CompositeDisposable;
  private disposed = false;
  get Activated(): Observable<void> { return this.activated.asObservable(); }
  get Deactivated(): Observable<void> { return this.deactivated.asObservable(); }
  get IsActive(): Observable<boolean> { return this.active.asObservable(); }
  get IsActiveValue(): boolean { return this.active.value; }
  get ReferenceCount(): number { return this.references; }
  private run(block: ActivationBlock, scope: CompositeDisposable): void {
    const childScope = new CompositeDisposable(); scope.Add(childScope);
    try { const result = block(childScope); if (result) childScope.Add(result); }
    catch (error) { scope.Remove(childScope); throw error; }
  }
  AddActivationBlock(block: ActivationBlock): IDisposable {
    if (this.disposed) throw new Error('Activator is disposed');
    const key = Symbol(); this.blocks.set(key, block);
    if (this.scope) {
      try { this.run(block, this.scope); }
      catch (error) { this.blocks.delete(key); throw error; }
    }
    return Disposable.Create(() => this.blocks.delete(key));
  }
  Activate(): IDisposable {
    if (this.disposed) throw new Error('Activator is disposed');
    if (this.references === 0) {
      const scope = new CompositeDisposable(); this.scope = scope; this.references = 1; this.generation++;
      try { for (const block of [...this.blocks.values()]) this.run(block, scope); }
      catch (error) { this.references = 0; this.scope = undefined; scope.Dispose(); throw error; }
      this.active.next(true); this.activated.next();
    } else this.references++;
    const generation = this.generation;
    return Disposable.Create(() => { if (generation === this.generation) this.Deactivate(); });
  }
  Deactivate(ignoreRefCount = false): void {
    if (!this.references) return;
    this.references = ignoreRefCount ? 0 : this.references - 1;
    if (this.references) return;
    const scope = this.scope; this.scope = undefined; this.generation++;
    try { scope?.Dispose(); }
    finally { this.active.next(false); this.deactivated.next(); }
  }
  Dispose(): void {
    if (this.disposed) return; this.disposed = true;
    try { this.Deactivate(true); }
    finally { this.blocks.clear(); this.active.complete(); this.activated.complete(); this.deactivated.complete(); }
  }
  unsubscribe(): void { this.Dispose(); }
}

export function WhenActivated(target: ViewModelActivator | IActivatableViewModel, block: ActivationBlock): IDisposable {
  return (target instanceof ViewModelActivator ? target : target.Activator).AddActivationBlock(block);
}
