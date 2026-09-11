import test from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorSubject, EMPTY, NEVER, Observable, Subject, firstValueFrom, lastValueFrom, map, of, take, throwError, toArray } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { ReactiveCommand, InvokeCommand } from '../dist/command.js';
import { RxApp } from '../dist/rx-app.js';

test('commands are cold per Execute subscription and also real RxJS Observables', async () => {
  let calls = 0;
  const command = ReactiveCommand.Create<number, number>(input => input + ++calls);
  const results: number[] = [];
  command.pipe(map(value => value * 10)).subscribe(value => results.push(value));
  const execution = command.Execute(10);
  assert.equal(calls, 0);
  assert.equal(await firstValueFrom(execution), 11);
  assert.equal(await firstValueFrom(execution), 12);
  assert.deepEqual(results, [110, 120]);
  command.Dispose();
});

test('explicit Execute runs while disabled and concurrent executions aggregate IsExecuting', () => {
  const gate = new BehaviorSubject(false);
  const one = new Subject<number>();
  const two = new Subject<number>();
  const command = ReactiveCommand.CreateFromObservable<number, number>(input => input === 1 ? one : two, gate);
  const state: boolean[] = [];
  command.IsExecuting.subscribe(value => state.push(value));
  assert.equal(command.CanExecuteValue, false);
  const a = command.Execute(1).subscribe();
  const b = command.Execute(2).subscribe();
  gate.next(true);
  assert.equal(command.CanExecuteValue, false);
  a.unsubscribe();
  assert.equal(command.IsExecutingValue, true);
  b.unsubscribe();
  assert.deepEqual(state, [false, true, false]);
  assert.equal(command.CanExecuteValue, true);
  command.Dispose();
});

test('can-execute starts false until its source emits and filters repeated states', () => {
  const gate = new Subject<boolean>();
  const command = ReactiveCommand.Create(() => 1, gate);
  const states: boolean[] = [];
  command.CanExecute.subscribe(value => states.push(value));
  gate.next(false); gate.next(true); gate.next(true);
  command.Execute().subscribe();
  assert.deepEqual(states, [false, true, false, true]);
  command.Dispose();
});

test('all observable results broadcast and an execution failure does not terminate the command', async () => {
  let calls = 0;
  const error = new Error('failure');
  const command = ReactiveCommand.CreateFromObservable(() => ++calls === 1 ? throwError(() => error) : of(1, 2, 3));
  const errors: unknown[] = [];
  const all: number[] = [];
  command.ThrownExceptions.subscribe(value => errors.push(value));
  command.subscribe(value => all.push(value));
  await assert.rejects(firstValueFrom(command.Execute()), error);
  assert.deepEqual(await lastValueFrom(command.Execute().pipe(toArray())), [1, 2, 3]);
  assert.deepEqual(all, [1, 2, 3]);
  assert.deepEqual(errors, [error]);
  assert.equal(command.CanExecuteValue, true);
  command.Dispose();
});

test('synchronously throwing observable and task factories restore execution state', async () => {
  for (const create of [ReactiveCommand.CreateFromObservable, ReactiveCommand.CreateFromTask]) {
    const error = new Error('factory');
    const command = create(() => { throw error; });
    const errors: unknown[] = [];
    command.ThrownExceptions.subscribe(value => errors.push(value));
    await assert.rejects(firstValueFrom(command.Execute()), error);
    assert.deepEqual(errors, [error]);
    assert.equal(command.IsExecutingValue, false);
    assert.equal(command.CanExecuteValue, true);
    command.Dispose();
  }
});

test('unobserved execution errors delegate to current RxApp default handler', async () => {
  const old = RxApp.DefaultExceptionHandler;
  const errors: unknown[] = [];
  RxApp.DefaultExceptionHandler = error => errors.push(error);
  try {
    const error = new Error('unobserved');
    const command = ReactiveCommand.Create(() => { throw error; });
    await assert.rejects(firstValueFrom(command.Execute()), error);
    assert.deepEqual(errors, [error]);
    command.Dispose();
  } finally { RxApp.DefaultExceptionHandler = old; }
});

test('can-execute errors reach ThrownExceptions and disable UI invocation', () => {
  const gate = new Subject<boolean>();
  const command = ReactiveCommand.Create(() => 1, gate);
  const errors: unknown[] = [];
  command.ThrownExceptions.subscribe(value => errors.push(value));
  gate.next(true);
  const error = new Error('gate');
  gate.error(error);
  assert.deepEqual(errors, [error]);
  assert.equal(command.CanExecuteValue, false);
  command.Dispose();
});

test('output scheduler controls broadcasts and state; Execute result remains synchronous', () => {
  const scheduler = new TestScheduler(() => {});
  const command = ReactiveCommand.Create(() => 7, undefined, scheduler);
  const raw: number[] = [];
  const output: number[] = [];
  const state: boolean[] = [];
  command.subscribe(value => output.push(value));
  command.IsExecuting.subscribe(value => state.push(value));
  command.Execute().subscribe(value => raw.push(value));
  assert.deepEqual(raw, [7]);
  assert.deepEqual(output, []);
  assert.deepEqual(state, [false]);
  scheduler.flush();
  assert.deepEqual(output, [7]);
  assert.deepEqual(state, [false, true, false]);
  command.Dispose();
});

test('observable cancellation tears down source and stops synchronous source emissions with take(1)', async () => {
  let emissions = 0;
  let cleaned = 0;
  let signal!: AbortSignal;
  const command = ReactiveCommand.CreateFromObservable<void, number>((_, abortSignal) => {
    signal = abortSignal;
    return new Observable(subscriber => {
      for (let i = 0; i < 5 && !subscriber.closed; i++) { emissions++; subscriber.next(i); }
      return () => { cleaned++; };
    });
  });
  assert.equal(await firstValueFrom(command.Execute().pipe(take(1))), 0);
  assert.equal(emissions, 1);
  assert.equal(cleaned, 1);
  assert.equal(signal.aborted, true);
  assert.equal(command.IsExecutingValue, false);
  command.Dispose();
});

test('task cancellation aborts signal and stays executing until underlying task settles', async () => {
  let resolve!: (value: number) => void;
  let signal!: AbortSignal;
  const command = ReactiveCommand.CreateFromTask<void, number>((_, abortSignal) => {
    signal = abortSignal;
    return new Promise(done => { resolve = done; });
  });
  const output: number[] = [];
  command.subscribe(value => output.push(value));
  const execution = command.Execute().subscribe();
  execution.unsubscribe();
  assert.equal(signal.aborted, true);
  assert.equal(command.IsExecutingValue, true);
  resolve(12);
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(command.IsExecutingValue, false);
  assert.deepEqual(output, []);
  command.Dispose();
});

test('task rejection after cancellation is consumed without a late output/error', async () => {
  let reject!: (reason: unknown) => void;
  const command = ReactiveCommand.CreateFromTask(() => new Promise((_done, fail) => { reject = fail; }));
  const errors: unknown[] = [];
  command.ThrownExceptions.subscribe(error => errors.push(error));
  const execution = command.Execute().subscribe();
  execution.unsubscribe();
  reject(new Error('AbortError'));
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(command.IsExecutingValue, false);
  assert.deepEqual(errors, []);
  command.Dispose();
});

test('InvokeCommand drops disabled and busy inputs without replaying them later', () => {
  const source = new Subject<number>();
  const gate = new BehaviorSubject(false);
  const active = new Subject<number>();
  const seen: number[] = [];
  const command = ReactiveCommand.CreateFromObservable<number, number>(input => { seen.push(input); return active; }, gate);
  const binding = InvokeCommand(source, command);
  source.next(1);
  gate.next(true);
  source.next(2);
  source.next(3);
  active.complete();
  assert.deepEqual(seen, [2]);
  source.next(4);
  assert.deepEqual(seen, [2, 4]);
  binding.Dispose();
  source.next(5);
  assert.deepEqual(seen, [2, 4]);
  command.Dispose();
});

test('InvokeCommand operator continues accepting events after execution failure', async () => {
  const command = ReactiveCommand.Create<number, number>(input => {
    if (input === 2) throw new Error('skip');
    return input * 2;
  });
  command.ThrownExceptions.subscribe();
  const results = await lastValueFrom(of(1, 2, 3).pipe(InvokeCommand(command), toArray()));
  assert.deepEqual(results, [2, 6]);
  command.Dispose();
});

test('combined command gates on every child and yields child results in input order', async () => {
  const gate = new BehaviorSubject(false);
  const first = ReactiveCommand.Create<number, number>(value => value + 1);
  const second = ReactiveCommand.Create<number, number>(value => value * 2, gate);
  const combined = ReactiveCommand.CreateCombined([first, second]);
  assert.equal(combined.CanExecuteValue, false);
  gate.next(true);
  assert.equal(combined.CanExecuteValue, true);
  assert.deepEqual(await firstValueFrom(combined.Execute(4)), [5, 8]);
  combined.Dispose();
  assert.equal(first.IsDisposed, false);
  first.Dispose(); second.Dispose();
});

test('combined command forwards each child failure once on scheduled outputs', async () => {
  const scheduler = new TestScheduler(() => {});
  const error = new Error('child');
  const child = ReactiveCommand.Create(() => { throw error; }, undefined, scheduler);
  const combined = ReactiveCommand.CreateCombined([child], undefined, scheduler);
  const errors: unknown[] = [];
  combined.ThrownExceptions.subscribe(value => errors.push(value));
  await assert.rejects(firstValueFrom(combined.Execute()), error);
  scheduler.flush();
  assert.deepEqual(errors, [error]);
  combined.Dispose(); child.Dispose();
});

test('combined command emits updated latest child values until all children complete', () => {
  const one = new Subject<number>();
  const two = new Subject<number>();
  const first = ReactiveCommand.CreateFromObservable(() => one);
  const second = ReactiveCommand.CreateFromObservable(() => two);
  const combined = ReactiveCommand.CreateCombined([first, second]);
  const output: number[][] = [];
  const execution = combined.Execute().subscribe(value => output.push(value));
  one.next(1); two.next(2); one.next(3); two.next(4);
  assert.deepEqual(output, [[1, 2], [3, 2], [3, 4]]);
  one.complete();
  assert.equal(execution.closed, false);
  two.complete();
  assert.equal(execution.closed, true);
  combined.Dispose(); first.Dispose(); second.Dispose();
});

test('combined requires children and empty execution completes without a value', async () => {
  assert.throws(() => ReactiveCommand.CreateCombined([]), /At least one/);
  const empty = ReactiveCommand.CreateFromObservable(() => EMPTY);
  await assert.rejects(empty.executeAsync(), /without a result/);
  empty.Dispose();
});

test('Dispose cancels active work, completes streams, and rejects future execution', async () => {
  const command = ReactiveCommand.CreateFromObservable(() => NEVER);
  let ended = false;
  let streamEnded = false;
  command.subscribe({ complete: () => { streamEnded = true; } });
  command.Execute().subscribe({ complete: () => { ended = true; } });
  command.Dispose(); command.Dispose();
  assert.equal(ended, true);
  assert.equal(streamEnded, true);
  assert.equal(command.IsExecutingValue, false);
  assert.equal(command.CanExecuteValue, false);
  await assert.rejects(firstValueFrom(command.Execute()), /disposed/);
});
