import test from 'node:test';
import assert from 'node:assert/strict';
import { EMPTY, NEVER, Observable, Subject, firstValueFrom, of } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { Interaction, InteractionContext, UnhandledInteractionError, UnhandledInteractionException } from '../dist/interaction.js';

test('interaction context carries input and allows exactly one output including undefined', () => {
  const context = new InteractionContext<string, undefined>('question');
  assert.equal(context.Input, 'question');
  assert.equal(context.GetInput(), 'question');
  assert.equal(context.IsHandled, false);
  assert.throws(() => context.GetOutput(), /not been set/);
  context.SetOutput(undefined);
  assert.equal(context.GetOutput(), undefined);
  assert.equal(context.IsHandled, true);
  assert.throws(() => context.SetOutput(undefined), /already been set/);
});

test('Handle is cold, and every subscription gets an independent context', async () => {
  let calls = 0;
  const interaction = new Interaction<string, number>();
  interaction.RegisterHandler(context => { assert.equal(context.Input, 'hello'); context.SetOutput(++calls); });
  const request = interaction.Handle('hello');
  assert.equal(calls, 0);
  assert.equal(await firstValueFrom(request), 1);
  assert.equal(await firstValueFrom(request), 2);
  interaction.Dispose();
});

test('handlers run newest-first, may decline, and stop after a handler answers', async () => {
  const order: string[] = [];
  const interaction = new Interaction<void, number>();
  interaction.RegisterHandler(context => { order.push('oldest'); context.SetOutput(0); });
  interaction.RegisterHandler(context => { order.push('middle'); context.SetOutput(4); });
  interaction.RegisterHandler(() => { order.push('newest'); });
  assert.equal(await firstValueFrom(interaction.Handle()), 4);
  assert.deepEqual(order, ['newest', 'middle']);
  interaction.Dispose();
});

test('unregistering a handler restores the previous handler and is idempotent', async () => {
  const interaction = new Interaction<number, number>();
  interaction.RegisterHandler(context => context.SetOutput(context.Input + 1));
  const registration = interaction.RegisterHandler(context => context.SetOutput(context.Input + 10));
  assert.equal(await firstValueFrom(interaction.Handle(1)), 11);
  registration.Dispose(); registration.unsubscribe();
  assert.equal(await firstValueFrom(interaction.Handle(1)), 2);
  assert.equal(interaction.HandlerCount, 1);
  interaction.Dispose();
});

test('duplicate handler registrations have independently disposable identities', () => {
  const interaction = new Interaction<void, number>();
  const handler = () => {};
  const first = interaction.RegisterHandler(handler);
  const second = interaction.RegisterHandler(handler);
  first.Dispose();
  assert.equal(interaction.HandlerCount, 1);
  second.Dispose();
  assert.equal(interaction.HandlerCount, 0);
  interaction.Dispose();
});

test('unhandled errors expose the original interaction and input', async () => {
  const interaction = new Interaction<string, boolean>();
  interaction.RegisterHandler(() => EMPTY);
  await assert.rejects(firstValueFrom(interaction.Handle('missing')), error => {
    assert.ok(error instanceof UnhandledInteractionError);
    assert.ok(error instanceof UnhandledInteractionException);
    assert.equal(error.Interaction, interaction);
    assert.equal(error.Input, 'missing');
    return true;
  });
  interaction.Dispose();
});

test('async handlers complete before fallback is invoked', async () => {
  const order: string[] = [];
  const interaction = new Interaction<number, string>();
  interaction.RegisterHandler(context => { order.push('fallback'); context.SetOutput('accepted'); });
  interaction.RegisterHandler(async context => {
    order.push('start');
    assert.equal(context.GetInput(), 3);
    await Promise.resolve();
    order.push('finish');
  });
  assert.equal(await firstValueFrom(interaction.Handle(3)), 'accepted');
  assert.deepEqual(order, ['start', 'finish', 'fallback']);
  interaction.Dispose();
});

test('an observable handler may emit many progress values but must complete', async () => {
  const progress = new Subject<number>();
  const interaction = new Interaction<void, string>();
  interaction.RegisterHandler(context => { context.SetOutput('done'); return progress; });
  const values: string[] = [];
  const subscription = interaction.Handle().subscribe(value => values.push(value));
  progress.next(1); progress.next(2);
  assert.deepEqual(values, []);
  progress.complete();
  assert.deepEqual(values, ['done']);
  assert.equal(subscription.closed, true);
  interaction.Dispose();
});

test('synchronous and asynchronous handler errors propagate without invoking fallback', async () => {
  for (const asynchronous of [false, true]) {
    const interaction = new Interaction<void, boolean>();
    const error = new Error('handler failed');
    let fallback = false;
    interaction.RegisterHandler(context => { fallback = true; context.SetOutput(true); });
    interaction.RegisterHandler(() => {
      if (asynchronous) return Promise.reject(error);
      throw error;
    });
    await assert.rejects(firstValueFrom(interaction.Handle()), error);
    assert.equal(fallback, false);
    interaction.Dispose();
  }
});

test('cancelling an interaction aborts its signal and disposes observable handler work', () => {
  const interaction = new Interaction<void, boolean>();
  let signal!: AbortSignal;
  let cleaned = false;
  interaction.RegisterHandler(context => {
    signal = context.Signal;
    return new Observable(() => () => { cleaned = true; });
  });
  const subscription = interaction.Handle().subscribe();
  subscription.unsubscribe();
  assert.equal(signal.aborted, true);
  assert.equal(cleaned, true);
  interaction.Dispose();
});

test('cancelling a queued interaction prevents handler invocation', () => {
  const scheduler = new TestScheduler(() => {});
  const interaction = new Interaction<void, boolean>(scheduler);
  let calls = 0;
  interaction.RegisterHandler(context => { calls++; context.SetOutput(true); });
  const subscription = interaction.Handle().subscribe();
  subscription.unsubscribe();
  scheduler.flush();
  assert.equal(calls, 0);
  interaction.Dispose();
});

test('async completion schedules fallback on the supplied handler scheduler', async () => {
  const scheduler = new TestScheduler(() => {});
  const interaction = new Interaction<void, number>(scheduler);
  let answer = 0;
  interaction.RegisterHandler(context => context.SetOutput(7));
  interaction.RegisterHandler(() => Promise.resolve());
  interaction.Handle().subscribe(value => { answer = value; });
  scheduler.flush();
  assert.equal(answer, 0);
  await Promise.resolve();
  scheduler.flush();
  assert.equal(answer, 7);
  interaction.Dispose();
});

test('handler snapshot is taken per subscription and unaffected by unregister during handling', async () => {
  const interaction = new Interaction<void, number>();
  const old = interaction.RegisterHandler(context => context.SetOutput(8));
  interaction.RegisterHandler(() => { old.Dispose(); });
  assert.equal(await firstValueFrom(interaction.Handle()), 8);
  await assert.rejects(firstValueFrom(interaction.Handle()), UnhandledInteractionError);
  interaction.Dispose();
});

test('thousands of synchronous declining handlers do not overflow the stack', async () => {
  const interaction = new Interaction<void, number>();
  interaction.RegisterHandler(context => context.SetOutput(9));
  for (let i = 0; i < 10000; i++) interaction.RegisterHandler(() => of(undefined));
  assert.equal(await firstValueFrom(interaction.Handle()), 9);
  interaction.Dispose();
});

test('Dispose completes active requests and rejects future requests and handlers', async () => {
  const interaction = new Interaction<void, number>();
  interaction.RegisterHandler(() => NEVER);
  let completed = false;
  interaction.Handle().subscribe({ complete: () => { completed = true; } });
  interaction.Dispose(); interaction.Dispose();
  assert.equal(completed, true);
  assert.equal(interaction.HandlerCount, 0);
  await assert.rejects(firstValueFrom(interaction.Handle()), /disposed/);
  assert.throws(() => interaction.RegisterHandler(() => {}), /disposed/);
});
