import {
  BehaviorSubject, EMPTY, Observable, Subject, Subscriber, Subscription,
  catchError, combineLatest, filter, from, map, mergeMap,
  of, withLatestFrom,
  type ObservableInput, type OperatorFunction, type SchedulerLike,
} from 'rxjs';
import { RxApp } from './rx-app.js';

/** The small command contract that bindings and framework adapters consume. */
export interface IReactiveCommand<TInput = void, TOutput = void> {
  readonly CanExecute: Observable<boolean>;
  readonly IsExecuting: Observable<boolean>;
  readonly ThrownExceptions: Observable<unknown>;
  Execute(input?: TInput): Observable<TOutput>;
  Dispose(): void;
  unsubscribe(): void;
}

export type CommandTask<TInput, TOutput> = (input: TInput, signal: AbortSignal) => PromiseLike<TOutput>;
export type CommandObservable<TInput, TOutput> = (input: TInput, signal: AbortSignal) => ObservableInput<TOutput>;

/**
 * A reusable RxJS command. Subscribe to the command for all successful results;
 * subscribe to Execute(input) to start one cold execution and observe its result.
 * Explicit executions are independent of CanExecute, as in ReactiveUI. Bindings
 * and InvokeCommand honor CanExecute and drop disabled/busy invocations.
 *
 * Result broadcasts, execution-state transitions and ThrownExceptions use the
 * output scheduler. Execute's own observer receives the source notifications
 * directly. Unsubscribing cancels an observable execution and aborts a task's
 * signal; a task stays executing until its promise actually settles.
 */
export class ReactiveCommand<TInput = void, TOutput = void> extends Observable<TOutput>
  implements IReactiveCommand<TInput, TOutput> {
  private readonly results: Subject<TOutput>;
  private readonly canExecuteState = new BehaviorSubject(false);
  private readonly executingState = new BehaviorSubject(false);
  private readonly exceptions = new Subject<unknown>();
  private readonly lifetime = new Subscription();
  private readonly scheduled = new Subscription();
  private readonly executions = new Set<() => void>();
  private userCanExecute = false;
  private inFlight = 0;
  private disposed = false;
  private forwardExecutionErrors = true;

  readonly CanExecute = this.canExecuteState.asObservable();
  readonly IsExecuting = this.executingState.asObservable();
  readonly ThrownExceptions = this.exceptions.asObservable();
  readonly Results: Observable<TOutput>;

  /** Compatibility hook; by default this delegates to RxApp's current handler. */
  static DefaultExceptionHandler: (error: unknown) => void = error => RxApp.HandleException(error);

  constructor(
    private readonly executeFactory: CommandObservable<TInput, TOutput>,
    canExecute?: Observable<boolean>,
    private readonly outputScheduler: SchedulerLike = RxApp.MainThreadScheduler,
    private readonly tracksPromise = false,
  ) {
    const results = new Subject<TOutput>();
    super(subscriber => results.subscribe(subscriber));
    if (typeof executeFactory !== 'function') throw new TypeError('execute must be a function.');
    this.results = results;
    this.Results = results.asObservable();
    this.lifetime.add((canExecute ?? of(true)).subscribe({
      next: value => {
        this.userCanExecute = Boolean(value);
        this.setCanExecute(this.CanExecuteValue);
      },
      error: error => {
        this.userCanExecute = false;
        this.setCanExecute(false);
        this.reportException(error);
      },
    }));
  }

  static Create<TInput = void, TOutput = void>(
    execute: (input: TInput) => TOutput,
    canExecute?: Observable<boolean>,
    outputScheduler?: SchedulerLike,
  ): ReactiveCommand<TInput, TOutput> {
    if (typeof execute !== 'function') throw new TypeError('execute must be a function.');
    return new ReactiveCommand(input => of(execute(input)), canExecute, outputScheduler);
  }

  static CreateFromTask<TInput = void, TOutput = void>(
    execute: CommandTask<TInput, TOutput>,
    canExecute?: Observable<boolean>,
    outputScheduler?: SchedulerLike,
  ): ReactiveCommand<TInput, TOutput> {
    return new ReactiveCommand(execute, canExecute, outputScheduler, true);
  }

  static CreateFromObservable<TInput = void, TOutput = void>(
    execute: CommandObservable<TInput, TOutput>,
    canExecute?: Observable<boolean>,
    outputScheduler?: SchedulerLike,
  ): ReactiveCommand<TInput, TOutput> {
    return new ReactiveCommand(execute, canExecute, outputScheduler);
  }

  /** Execute every child, combining the latest result from each child. */
  static CreateCombined<TInput = void, TOutput = void>(
    commands: readonly ReactiveCommand<TInput, TOutput>[],
    canExecute?: Observable<boolean>,
    outputScheduler?: SchedulerLike,
  ): ReactiveCommand<TInput, TOutput[]> {
    if (commands == null) throw new TypeError('commands are required.');
    const children = [...commands];
    if (!children.length) throw new RangeError('At least one child command is required.');
    const enabled = combineLatest([
      canExecute ?? of(true),
      ...(children.map(command => command.CanExecute)),
    ]).pipe(map(states => states.every(Boolean)));
    const combined = ReactiveCommand.CreateFromObservable<TInput, TOutput[]>(
      input => combineLatest(children.map(command => command.Execute(input))),
      enabled,
      outputScheduler,
    );
    // Child errors are the sole aggregate exception source, including when a
    // child runs independently. Avoid reporting the same failure again when
    // combineLatest propagates it through the aggregate execution subscription.
    combined.forwardExecutionErrors = false;
    for (const child of children) {
      combined.lifetime.add(child.ThrownExceptions.subscribe(error => {
        combined.reportException(error);
      }));
    }
    return combined;
  }

  static create = ReactiveCommand.Create;
  static createFromTask = ReactiveCommand.CreateFromTask;
  static createFromObservable = ReactiveCommand.CreateFromObservable;
  static createCombined = ReactiveCommand.CreateCombined;

  get CanExecuteValue(): boolean { return !this.disposed && this.userCanExecute && this.inFlight === 0; }
  get IsExecutingValue(): boolean { return this.inFlight > 0; }
  get IsDisposed(): boolean { return this.disposed; }
  get canExecute$(): Observable<boolean> { return this.CanExecute; }
  get isExecuting$(): Observable<boolean> { return this.IsExecuting; }
  get thrownExceptions$(): Observable<unknown> { return this.ThrownExceptions; }

  Execute(input?: TInput): Observable<TOutput> {
    return new Observable<TOutput>(downstream => {
      if (this.disposed) {
        const error = new Error('ReactiveCommand has been disposed.');
        error.name = 'ObjectDisposedError';
        downstream.error(error);
        return;
      }
      const controller = new AbortController();
      let ended = false;
      let cancelled = false;
      let taskSettled = false;
      let taskStarted = false;
      const inner = new Subscription();
      const finish = () => {
        if (ended) return;
        ended = true;
        this.executions.delete(cancel);
        this.inFlight = Math.max(0, this.inFlight - 1);
        this.publishState();
      };
      const cancel = () => {
        if (!cancelled) {
          cancelled = true;
          controller.abort();
          inner.unsubscribe();
        }
        if (!taskStarted || taskSettled || this.disposed) finish();
        if (!downstream.closed) downstream.complete();
      };
      this.executions.add(cancel);
      this.inFlight++;
      this.publishState();
      // Install cancellation before invoking user code; this also handles a
      // synchronous take(1) unsubscribe during source notification correctly.
      downstream.add(cancel);

      const value = (result: TOutput) => {
        if (cancelled || downstream.closed || this.disposed) return;
        downstream.next(result);
        this.schedule(() => this.results.next(result));
      };
      const fail = (error: unknown) => {
        finish();
        if (cancelled || downstream.closed || this.disposed) return;
        if (this.forwardExecutionErrors) this.reportException(error);
        downstream.error(error);
      };
      try {
        if (downstream.closed || this.disposed) return;
        const source = this.executeFactory(input as TInput, controller.signal);
        if (this.tracksPromise) {
          if (source == null || typeof (source as PromiseLike<TOutput>).then !== 'function') {
            throw new TypeError('CreateFromTask must return a Promise or thenable.');
          }
          taskStarted = true;
          Promise.resolve(source as PromiseLike<TOutput>).then(
            result => {
              taskSettled = true;
              value(result);
              finish();
              downstream.complete();
            },
            error => {
              taskSettled = true;
              fail(error);
            },
          );
        } else {
          const observer = new Subscriber<TOutput>({
            next: value,
            error: fail,
            complete: () => { finish(); downstream.complete(); },
          });
          inner.add(observer);
          from(source).subscribe(observer);
        }
      } catch (error) { fail(error); }
    });
  }

  /** Gated, eager execution for DOM events and ICommand-style use. */
  Invoke(input?: TInput): Subscription {
    return this.CanExecuteValue
      ? this.Execute(input).subscribe({ error: () => { /* ThrownExceptions owns errors. */ } })
      : Subscription.EMPTY;
  }

  execute(input?: TInput): Observable<TOutput> { return this.Execute(input); }
  invoke(input?: TInput): Subscription { return this.Invoke(input); }

  executeAsync(input?: TInput): Promise<TOutput> {
    return new Promise<TOutput>((resolve, reject) => {
      let found = false;
      let last!: TOutput;
      this.Execute(input).subscribe({
        next: value => { found = true; last = value; },
        error: reject,
        complete: () => found ? resolve(last) : reject(new Error('The command completed without a result.')),
      });
    });
  }

  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.lifetime.unsubscribe();
    for (const cancel of [...this.executions]) cancel();
    this.scheduled.unsubscribe();
    this.inFlight = 0;
    this.setCanExecute(false);
    if (this.executingState.value) this.executingState.next(false);
    this.canExecuteState.complete();
    this.executingState.complete();
    this.results.complete();
    this.exceptions.complete();
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }

  private setCanExecute(value: boolean): void {
    if (this.canExecuteState.value !== value) this.canExecuteState.next(value);
  }

  private publishState(): void {
    const executing = this.IsExecutingValue;
    this.schedule(() => {
      if (this.executingState.value !== executing) this.executingState.next(executing);
      this.setCanExecute(!this.disposed && this.userCanExecute && !executing);
    });
  }

  private reportException(error: unknown): void {
    this.schedule(() => {
      if (this.exceptions.observed) this.exceptions.next(error);
      else ReactiveCommand.DefaultExceptionHandler(error);
    });
  }

  private schedule(action: () => void): void {
    if (this.disposed) return;
    // Self-removing actions avoid retaining every completed scheduled result.
    const scheduled = this.outputScheduler.schedule(function () { action(); this.unsubscribe(); });
    this.scheduled.add(scheduled);
  }
}

/** Operator form: source.pipe(InvokeCommand(command)).subscribe(). */
export function InvokeCommand<TInput, TOutput>(command: ReactiveCommand<TInput, TOutput>): OperatorFunction<TInput, TOutput>;
/** .NET extension-style form: InvokeCommand(source, command), already subscribed. */
export function InvokeCommand<TInput, TOutput>(source: Observable<TInput>, command: ReactiveCommand<TInput, TOutput>): Subscription & { Dispose(): void };
export function InvokeCommand<TInput, TOutput>(
  sourceOrCommand: Observable<TInput> | ReactiveCommand<TInput, TOutput>,
  command?: ReactiveCommand<TInput, TOutput>,
): OperatorFunction<TInput, TOutput> | (Subscription & { Dispose(): void }) {
  const target = command ?? sourceOrCommand as ReactiveCommand<TInput, TOutput>;
  const operator: OperatorFunction<TInput, TOutput> = source => source.pipe(
    withLatestFrom(target.CanExecute),
    filter(([, enabled]) => enabled && target.CanExecuteValue),
    mergeMap(([input]) => target.Execute(input).pipe(catchError(() => EMPTY))),
  );
  if (!command) return operator;
  const subscription = operator(sourceOrCommand as Observable<TInput>).subscribe({
    error: error => ReactiveCommand.DefaultExceptionHandler(error),
  });
  return Object.assign(subscription, { Dispose() { subscription.unsubscribe(); } });
}

export const invokeCommand = InvokeCommand;
