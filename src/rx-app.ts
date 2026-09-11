import { asyncScheduler, queueScheduler, type Observer, type SchedulerLike } from 'rxjs';

export interface RxAppOptions {
  mainThreadScheduler?: SchedulerLike;
  taskpoolScheduler?: SchedulerLike;
  defaultExceptionHandler?: ((error: unknown) => void) | Pick<Observer<unknown>, 'next'>;
}

/** Application-wide scheduling defaults. Override UI dispatch before constructing view models. */
export class RxApp {
  static MainThreadScheduler: SchedulerLike = queueScheduler;
  static TaskpoolScheduler: SchedulerLike = asyncScheduler;
  static DefaultExceptionHandler: ((error: unknown) => void) | Pick<Observer<unknown>, 'next'> = {
    next(error) { console.error('Unhandled ReactiveWeb exception:', error); }
  };
  static Configure(options: RxAppOptions): void {
    if (options.mainThreadScheduler) this.MainThreadScheduler = options.mainThreadScheduler;
    if (options.taskpoolScheduler) this.TaskpoolScheduler = options.taskpoolScheduler;
    if (options.defaultExceptionHandler) this.DefaultExceptionHandler = options.defaultExceptionHandler;
  }
  static configure(options: RxAppOptions): void { this.Configure(options); }
  static HandleException(error: unknown): void {
    const handler = this.DefaultExceptionHandler;
    if (typeof handler === 'function') handler(error);
    else handler.next(error);
  }
  static get mainThreadScheduler(): SchedulerLike { return this.MainThreadScheduler; }
  static set mainThreadScheduler(value: SchedulerLike) { this.MainThreadScheduler = value; }
  static get taskpoolScheduler(): SchedulerLike { return this.TaskpoolScheduler; }
  static set taskpoolScheduler(value: SchedulerLike) { this.TaskpoolScheduler = value; }
}
