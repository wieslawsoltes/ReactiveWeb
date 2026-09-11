import {
  Observable, Subscriber, Subscription, from, of, queueScheduler,
  type ObservableInput, type SchedulerLike,
} from 'rxjs';

export interface IInteractionContext<TInput, TOutput> {
  readonly Input: TInput;
  readonly IsHandled: boolean;
  readonly Signal: AbortSignal;
  GetInput(): TInput;
  SetOutput(output: TOutput): void;
}

/** One request and its write-once answer. */
export class InteractionContext<TInput, TOutput> implements IInteractionContext<TInput, TOutput> {
  private handled = false;
  private output!: TOutput;
  readonly Signal: AbortSignal;
  constructor(readonly Input: TInput, signal?: AbortSignal) {
    this.Signal = signal ?? new AbortController().signal;
  }
  get IsHandled(): boolean { return this.handled; }
  GetInput(): TInput { return this.Input; }
  SetOutput(output: TOutput): void {
    if (this.handled) throw new Error('Output has already been set.');
    this.output = output;
    this.handled = true;
  }
  GetOutput(): TOutput {
    if (!this.handled) throw new Error('Output has not been set.');
    return this.output;
  }
  get input(): TInput { return this.Input; }
  get isHandled(): boolean { return this.IsHandled; }
  get signal(): AbortSignal { return this.Signal; }
  setOutput(output: TOutput): void { this.SetOutput(output); }
}

export class UnhandledInteractionError<TInput = unknown, TOutput = unknown> extends Error {
  constructor(readonly Interaction: Interaction<TInput, TOutput>, readonly Input: TInput) {
    super('No registered interaction handler provided an output.');
    this.name = 'UnhandledInteractionError';
  }
}
/** Familiar .NET name, referring to the same error class. */
export { UnhandledInteractionError as UnhandledInteractionException };

export type InteractionHandler<TInput, TOutput> = (
  context: InteractionContext<TInput, TOutput>,
) => void | PromiseLike<unknown> | ObservableInput<unknown>;

export interface IInteraction<TInput, TOutput> {
  Handle(input: TInput): Observable<TOutput>;
  RegisterHandler(handler: InteractionHandler<TInput, TOutput>): Subscription & { Dispose(): void };
}

/**
 * A view-independent request/response channel. Handlers are tried newest first,
 * sequentially; each may decline by completing without SetOutput. Async and
 * Observable handlers must complete before the next handler runs. Every Handle
 * subscription takes a fresh handler snapshot and independent context.
 */
export class Interaction<TInput, TOutput> implements IInteraction<TInput, TOutput> {
  private handlers: Array<{ handler: InteractionHandler<TInput, TOutput> }> = [];
  private readonly requests = new Set<() => void>();
  private disposed = false;

  constructor(private readonly handlerScheduler: SchedulerLike = queueScheduler) {}

  RegisterHandler(handler: InteractionHandler<TInput, TOutput>): Subscription & { Dispose(): void } {
    if (typeof handler !== 'function') throw new TypeError('handler must be a function.');
    if (this.disposed) throw new Error('Interaction has been disposed.');
    // Entries have unique identity so registering the same function twice can
    // still be independently disposed.
    const entry = { handler };
    this.handlers.push(entry);
    const subscription = new Subscription(() => {
      const index = this.handlers.indexOf(entry);
      if (index >= 0) this.handlers.splice(index, 1);
    });
    return Object.assign(subscription, { Dispose() { subscription.unsubscribe(); } });
  }

  Handle(input: TInput): Observable<TOutput> {
    return new Observable<TOutput>(downstream => {
      if (this.disposed) { downstream.error(new Error('Interaction has been disposed.')); return; }
      const handlers = this.handlers.slice().reverse();
      const controller = new AbortController();
      const context = this.GenerateContext(input, controller.signal);
      const lifetime = new Subscription();
      let index = 0;
      const cancel = () => {
        controller.abort();
        lifetime.unsubscribe();
        this.requests.delete(cancel);
        if (!downstream.closed) downstream.complete();
      };
      this.requests.add(cancel);
      downstream.add(cancel);
      const step = () => {
        const work = this.handlerScheduler.schedule(() => {
          if (downstream.closed) return;
          if (context.IsHandled) {
            downstream.next(context.GetOutput());
            downstream.complete();
            return;
          }
          if (index >= handlers.length) {
            downstream.error(new UnhandledInteractionError(this, input));
            return;
          }
          try {
            const result = handlers[index++]!.handler(context);
            if (downstream.closed) return;
            const observer = new Subscriber<unknown>({
              next: () => {},
              error: (error: unknown) => downstream.error(error),
              complete: step,
            });
            lifetime.add(observer);
            (result == null ? of(undefined) : from(result)).subscribe(observer);
          } catch (error) { downstream.error(error); }
        });
        lifetime.add(work);
      };
      step();
    });
  }

  protected GenerateContext(input: TInput, signal?: AbortSignal): InteractionContext<TInput, TOutput> {
    return new InteractionContext(input, signal);
  }

  get IsDisposed(): boolean { return this.disposed; }
  get HandlerCount(): number { return this.handlers.length; }
  handle(input: TInput): Observable<TOutput> { return this.Handle(input); }
  registerHandler(handler: InteractionHandler<TInput, TOutput>): Subscription & { Dispose(): void } {
    return this.RegisterHandler(handler);
  }
  Dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.handlers = [];
    for (const cancel of [...this.requests]) cancel();
  }
  unsubscribe(): void { this.Dispose(); }
  dispose(): void { this.Dispose(); }
}
