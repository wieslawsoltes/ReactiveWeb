import test from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorSubject, Observable, Subject, firstValueFrom, lastValueFrom, of, queueScheduler, throwError, toArray } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { ScheduledSubject, WaitForDispatcherScheduler } from '../dist/scheduled-subject.js';
import { Do, Log, LoggedCatch, SwitchSelect, SwitchSubscribe } from '../dist/observable-extensions.js';
import { RxApp } from '../dist/rx-app.js';

test('ScheduledSubject schedules next and completion and keeps notification order', () => {
  const scheduler = new TestScheduler(() => {});
  const subject = new ScheduledSubject<number>(scheduler);
  const values: (number | string)[] = [];
  subject.Subscribe({ next: value => values.push(value), complete: () => values.push('done') });
  subject.OnNext(1); subject.OnNext(2); subject.OnCompleted();
  assert.deepEqual(values, []);
  scheduler.flush();
  assert.deepEqual(values, [1, 2, 'done']);
  assert.equal(subject.ObserverCount, 0);
  subject.Dispose();
});

test('ScheduledSubject sends unobserved values to default observer and restores it after unsubscribe', () => {
  const defaults: number[] = [];
  const real: number[] = [];
  const subject = new ScheduledSubject<number>(queueScheduler, value => defaults.push(value));
  assert.equal(subject.HasObservers, true);
  subject.next(1);
  const subscription = subject.Subscribe(value => real.push(value));
  subject.next(2);
  subscription.Dispose(); subscription.unsubscribe();
  subject.next(3);
  assert.deepEqual(defaults, [1, 3]);
  assert.deepEqual(real, [2]);
  assert.equal(subject.ObserverCount, 0);
  subject.Dispose();
});

test('ScheduledSubject cancels pending default delivery when a real subscriber takes over', () => {
  const scheduler = new TestScheduler(() => {});
  const defaults: number[] = [];
  const real: number[] = [];
  const subject = new ScheduledSubject<number>(scheduler, value => defaults.push(value));
  subject.next(1);
  subject.subscribe(value => real.push(value));
  subject.next(2);
  scheduler.flush();
  assert.deepEqual(defaults, []);
  assert.deepEqual(real, [2]);
  subject.Dispose();
});

test('ScheduledSubject supports a replaying backing subject and schedules its initial value', () => {
  const scheduler = new TestScheduler(() => {});
  const subject = new ScheduledSubject(scheduler, undefined, new BehaviorSubject(7));
  const values: number[] = [];
  subject.subscribe(value => values.push(value));
  assert.deepEqual(values, []);
  scheduler.flush();
  assert.deepEqual(values, [7]);
  subject.Dispose();
});

test('ScheduledSubject schedules errors and its default missing error callback uses RxApp', () => {
  const scheduler = new TestScheduler(() => {});
  const error = new Error('subject failed');
  const errors: unknown[] = [];
  const subject = new ScheduledSubject<number>(scheduler);
  subject.subscribe({ error: value => errors.push(value) });
  subject.OnError(error);
  assert.equal(errors.length, 0);
  scheduler.flush();
  assert.equal(errors.length, 1);
  assert.equal(errors[0], error);
  subject.Dispose();
  const old = RxApp.DefaultExceptionHandler;
  RxApp.DefaultExceptionHandler = value => errors.push(value);
  try {
    const fallback = new ScheduledSubject<number>(queueScheduler, () => {});
    fallback.error(error);
    assert.deepEqual(errors, [error, error]);
    fallback.Dispose();
  } finally { RxApp.DefaultExceptionHandler = old; }
});

test('ScheduledSubject disposal cancels pending values and errors', () => {
  const scheduler = new TestScheduler(() => {});
  const subject = new ScheduledSubject<number>(scheduler);
  let notifications = 0;
  const subscription = subject.subscribe({ next: () => notifications++, error: () => notifications++ });
  subject.next(1); subject.error(new Error('cancelled'));
  subject.Dispose(); subject.Dispose();
  scheduler.flush();
  assert.equal(notifications, 0);
  assert.equal(subscription.closed, true);
  assert.equal(subject.HasObservers, false);
  assert.equal(subject.IsDisposed, true);
  assert.throws(() => subject.next(2), /disposed/);
});

test('dispatcher retries on use, falls back immediately and caches successful provider', () => {
  const target = new TestScheduler(() => {});
  let available = false;
  let tries = 0;
  const scheduler = new WaitForDispatcherScheduler(() => { tries++; return available ? target : undefined; });
  let calls = 0;
  scheduler.schedule(() => calls++);
  assert.equal(calls, 1);
  assert.equal(scheduler.IsReady, false);
  available = true;
  scheduler.schedule(() => calls++);
  assert.equal(calls, 1);
  target.flush();
  assert.equal(calls, 2);
  assert.equal(scheduler.IsReady, true);
  const cached = tries;
  scheduler.now(); scheduler.schedule(() => {});
  assert.equal(tries, cached);
  scheduler.Dispose();
});

test('dispatcher provider failures are observable and retried on later requests', () => {
  const error = new Error('not ready');
  let ready = false;
  const scheduler = new WaitForDispatcherScheduler(() => {
    if (!ready) throw error;
    return queueScheduler;
  });
  assert.equal(scheduler.LastProviderError, error);
  ready = true;
  scheduler.schedule(() => {});
  assert.equal(scheduler.IsReady, true);
  assert.equal(scheduler.LastProviderError, undefined);
  scheduler.Dispose();
});

test('dispatcher wait mode performs delayed retries then executes with remaining due time', () => {
  const clock = new TestScheduler(() => {});
  let ready = false;
  let tries = 0;
  const scheduler = new WaitForDispatcherScheduler(() => {
    tries++; return ready ? clock : undefined;
  }, { waitForDispatcher: true, retryScheduler: clock, retryDelay: 10 });
  const times: number[] = [];
  scheduler.schedule(() => times.push(clock.now()), 50);
  clock.schedule(() => { ready = true; }, 25);
  assert.equal(tries, 2);
  clock.flush();
  assert.deepEqual(times, [50]);
  assert.equal(tries, 5);
  scheduler.Dispose();
});

test('dispatcher wait mode cancellation stops retries and scheduler Dispose cancels queued work', () => {
  const clock = new TestScheduler(() => {});
  let tries = 0;
  const scheduler = new WaitForDispatcherScheduler(() => { tries++; return undefined; }, {
    waitForDispatcher: true, retryScheduler: clock, retryDelay: 10,
  });
  let calls = 0;
  const action = scheduler.schedule(() => calls++);
  clock.schedule(() => action.unsubscribe(), 25);
  clock.flush();
  assert.equal(tries, 4);
  assert.equal(calls, 0);
  scheduler.schedule(() => calls++);
  scheduler.Dispose();
  const before = tries;
  clock.flush();
  assert.equal(tries, before);
  assert.throws(() => scheduler.schedule(() => {}), /disposed/);
  assert.throws(() => new WaitForDispatcherScheduler(() => undefined, { retryDelay: 0 }), /positive/);
});

test('dispatcher wait mode supports recursively scheduled RxJS actions', () => {
  const clock = new TestScheduler(() => {});
  const scheduler = new WaitForDispatcherScheduler(() => clock, { waitForDispatcher: true, retryScheduler: clock });
  const values: number[] = [];
  scheduler.schedule(function (value: number) {
    values.push(value);
    if (value < 3) this.schedule(value + 1, 10);
    else this.unsubscribe();
  }, 0, 1);
  clock.flush();
  assert.deepEqual(values, [1, 2, 3]);
  scheduler.Dispose();
});

test('SwitchSubscribe cancels replaced inner and ignores null outer without detaching current', () => {
  const outer = new Subject<Observable<number> | null>();
  const first = new Subject<number>();
  const second = new Subject<number>();
  const values: number[] = [];
  const subscription = SwitchSubscribe(outer, value => values.push(value));
  outer.next(first); first.next(1);
  outer.next(null); first.next(2);
  outer.next(second); first.next(3); second.next(4);
  assert.deepEqual(values, [1, 2, 4]);
  assert.equal(first.observed, false);
  subscription.Dispose();
  assert.equal(second.observed, false);
  assert.equal(outer.observed, false);
});

test('SwitchSelect projection follows replaceable object and waits for last inner completion', () => {
  const outer = new Subject<{ values: Subject<number> }>();
  const inner = new Subject<number>();
  const values: number[] = [];
  let complete = false;
  const subscription = SwitchSubscribe(outer, value => value.values, {
    next: value => values.push(value), complete: () => { complete = true; },
  });
  outer.next({ values: inner });
  outer.complete(); inner.next(5);
  assert.equal(complete, false);
  inner.complete();
  assert.equal(complete, true);
  assert.equal(subscription.closed, true);
  assert.deepEqual(values, [5]);
});

test('SwitchSelect selector failure propagates and unsubscribes source', async () => {
  const error = new Error('selector');
  await assert.rejects(firstValueFrom(of(1).pipe(SwitchSelect(() => { throw error; }))), error);
});

test('Do side effects preserve values and completion; callback errors replace the notification', async () => {
  const effects: (number | string)[] = [];
  assert.deepEqual(await lastValueFrom(of(1, 2).pipe(Do<number>({
    next: value => effects.push(value), complete: () => effects.push('done'),
  }), toArray())), [1, 2]);
  assert.deepEqual(effects, [1, 2, 'done']);
  const error = new Error('side effect');
  await assert.rejects(firstValueFrom(Do(of(1), () => { throw error; })), error);
});

test('Log formats every event and preserves the original error', async () => {
  const entries: unknown[][] = [];
  const logger = { info: (...args: unknown[]) => entries.push(args), warn: (...args: unknown[]) => entries.push(args) };
  assert.deepEqual(await lastValueFrom(Log(of(1, 2), logger, 'read', value => `#${value}`).pipe(toArray())), [1, 2]);
  assert.deepEqual(entries, [['read OnNext', '#1'], ['read OnNext', '#2'], ['read OnCompleted', undefined]]);
  const error = new Error('source');
  await assert.rejects(firstValueFrom(throwError(() => error).pipe(Log(logger, 'read'))), error);
  assert.deepEqual(entries.at(-1), ['read OnError', error]);
});

test('LoggedCatch logs source error, switches to fallback, and does not catch fallback failure again', async () => {
  const errors: unknown[] = [];
  const logger = { warn: (_message: string, error: unknown) => errors.push(error) };
  const error = new Error('source');
  const fallbackError = new Error('fallback');
  assert.equal(await firstValueFrom(throwError(() => error).pipe(LoggedCatch(logger, of(8), 'recovered'))), 8);
  assert.equal(await firstValueFrom(LoggedCatch(throwError(() => error), logger)), undefined);
  await assert.rejects(firstValueFrom(throwError(() => error).pipe(LoggedCatch(logger, () => throwError(() => fallbackError)))), fallbackError);
  assert.deepEqual(errors, [error, error, error]);
});

test('LoggedCatch predicate preserves unmatched errors without logging', async () => {
  let logs = 0;
  const logger = { warn: () => logs++ };
  const error = new RangeError('unmatched');
  await assert.rejects(firstValueFrom(LoggedCatch(throwError(() => error), logger, of(2), '', value => value instanceof TypeError)), error);
  assert.equal(logs, 0);
});

test('.NET-style logger adapters work and thrown logger exceptions enter the observable error channel', async () => {
  const values: unknown[] = [];
  const logger = { Log: () => ({ Info: (message: string, value: unknown) => values.push([message, value]) }) };
  assert.equal(await firstValueFrom(of(3).pipe(Log(logger))), 3);
  assert.deepEqual(values, [['OnNext', 3]]);
  const error = new Error('logger failed');
  await assert.rejects(firstValueFrom(of(1).pipe(Log({ info() { throw error; } }))), error);
});
