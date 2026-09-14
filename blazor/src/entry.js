import * as engine from '../../dist/index.js';
import * as Html from '../../dist/html.js';
import * as Rx from 'rxjs';
import * as RxOperators from 'rxjs/operators';
export function ObserveReactiveValue(model, path) { return model.WhenAnyValue(path).pipe(RxOperators.map(value => ({ kind: 'value', value }))); }
export function ObserveReactiveChanges(model) { return model.Changed.pipe(RxOperators.map(({ propertyName, value, oldValue }) => ({ propertyName, value, oldValue }))); }
export function SetReactiveValues(model, values) {
  const scope = model.DelayChangeNotifications();
  try { for (const [name, value] of Object.entries(values)) model.SetValue(name, value); }
  finally { scope.Dispose(); }
}
/** Task-oriented facade retaining the native command's execution, observable state and cancellation. */
export class BlazorReactiveCommand {
  constructor(execute, options = {}) {
    this.enabled = new Rx.BehaviorSubject(options.enabled !== false); this.active = new Set(); this.disposed = false;
    const callback = options.passSignal ? execute : input => execute(input);
    this.Command = options.asynchronous !== false ? engine.ReactiveCommand.CreateFromTask(callback, this.enabled) : engine.ReactiveCommand.Create(callback, this.enabled);
    // Errors are observed by the returned promise and the public error stream, not an unhandled global handler.
    this.errors = this.Command.ThrownExceptions.subscribe(error => { this.LastError = error; });
  }
  get CanExecute() { return this.Command.CanExecute; }
  get IsExecuting() { return this.Command.IsExecuting; }
  get Results() { return this.Command.Results.pipe(RxOperators.map(value => ({ kind: 'value', value }))); }
  get ThrownExceptions() { return this.Command.ThrownExceptions; }
  get CanExecuteValue() { return this.Command.CanExecuteValue; }
  get IsExecutingValue() { return this.Command.IsExecutingValue; }
  SetEnabled(value) { if (this.disposed) throw new Error('Command is disposed.'); this.enabled.next(Boolean(value)); }
  ExecuteAsync(input) {
    if (!this.CanExecuteValue) return Promise.reject(new Error('Command is disabled, executing or disposed.'));
    return new Promise((resolve, reject) => {
      let subscription, ended = false, found = false, last;
      const finish = action => { if (ended) return; ended = true; this.active.delete(execution); action(); };
      const execution = { cancel: () => { finish(() => reject(new DOMException('Command execution was cancelled.', 'AbortError'))); subscription?.unsubscribe(); } };
      this.active.add(execution);
      subscription = this.Command.Execute(input).subscribe({
        next: value => { found = true; last = value; },
        error: error => finish(() => reject(error)),
        complete: () => finish(() => found ? resolve(last) : reject(new Error('The command completed without a result.')))
      });
    });
  }
  Cancel() { for (const execution of [...this.active]) execution.cancel(); }
  Dispose() { if (this.disposed) return; this.disposed = true; this.Cancel(); this.Command.Dispose(); this.errors.unsubscribe(); this.enabled.complete(); }
}
export const api = { ...engine, Html, Rx, RxOperators, ObserveReactiveValue, ObserveReactiveChanges, SetReactiveValues, BlazorReactiveCommand };
