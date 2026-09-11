import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createElement, StrictMode, act } from 'react';
import { renderToString } from 'react-dom/server';
import { createRoot } from 'react-dom/client';
import { Window } from 'happy-dom';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { ReactiveObject } from '../dist/reactive-object.js';
import { ViewModelActivator, WhenActivated } from '../dist/activation.js';
import { ReactiveProvider, useObservable, useReactiveObject, useWhenActivated, useViewModel, useReactiveCommand, RoutedViewHost } from '../dist/react.js';
import { ViewLocator } from '../dist/services.js';
import { ReactiveCommand } from '../dist/command.js';

const window = new Window();
Object.assign(globalThis, { window, document: window.document, HTMLElement: window.HTMLElement, IS_REACT_ACT_ENVIRONMENT: true });

test('useObservable SSR returns the explicit server snapshot without subscribing', () => {
  let subscribed = 0;
  const source = new Observable<number>(subscriber => { subscribed++; subscriber.next(42); });
  function View() { const value = useObservable(source, 1, 7); return createElement('span', null, String(value)); }
  assert.equal(renderToString(createElement(View)), '<span>7</span>');
  assert.equal(subscribed, 0);
});

test('useObservable handles StrictMode lifetimes and observable replacement', async () => {
  const one = new BehaviorSubject('one'), two = new BehaviorSubject('two');
  function View({ source }: { source: Observable<string> }) { return createElement('p', null, useObservable(source, 'initial')); }
  const container = window.document.createElement('div');
  const root = createRoot(container as any);
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { source: one }))));
  assert.equal(container.textContent, 'one'); assert.equal(one.observers.length, 1);
  await act(async () => one.next('changed')); assert.equal(container.textContent, 'changed');
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { source: two }))));
  assert.equal(container.textContent, 'two'); assert.equal(one.observers.length, 0); assert.equal(two.observers.length, 1);
  await act(async () => root.unmount()); assert.equal(two.observers.length, 0);
});

test('useReactiveObject updates multiple consumers and context replacement', async () => {
  const first = new ReactiveObject({ Name: 'Ada' }) as any;
  const second = new ReactiveObject({ Name: 'Grace' }) as any;
  function View() { const vm = useReactiveObject(useViewModel<any>()); return createElement('p', null, vm.Name); }
  const container = window.document.createElement('div'); const root = createRoot(container as any);
  const render = (viewModel: any) => createElement(ReactiveProvider, { viewModel }, createElement(View), createElement(View));
  await act(async () => root.render(render(first))); assert.equal(container.textContent, 'AdaAda');
  await act(async () => { first.Name = 'Changed'; }); assert.equal(container.textContent, 'ChangedChanged');
  await act(async () => root.render(render(second))); assert.equal(container.textContent, 'GraceGrace');
  await act(async () => { first.Name = 'stale'; }); assert.equal(container.textContent, 'GraceGrace');
  await act(async () => root.unmount()); first.Dispose(); second.Dispose();
});

test('useWhenActivated releases every StrictMode lease and callback resource', async () => {
  const viewModel = { Activator: new ViewModelActivator() };
  let setup = 0, cleanup = 0;
  function View() { useWhenActivated(viewModel, lifetime => { setup++; lifetime.Add(() => cleanup++); }); return null; }
  const container = window.document.createElement('div'); const root = createRoot(container as any);
  await act(async () => root.render(createElement(StrictMode, null, createElement(View))));
  assert.equal(viewModel.Activator.ReferenceCount, 1);
  await act(async () => root.unmount());
  assert.equal(viewModel.Activator.ReferenceCount, 0); assert.equal(setup, cleanup); assert.ok(setup >= 1);
});

test('useReactiveCommand returns results and rejects disabled execution', async () => {
  const enabled = new BehaviorSubject(true), executing = new BehaviorSubject(false);
  let hook: ReturnType<typeof useReactiveCommand<number, number>>;
  function View() { hook = useReactiveCommand({ CanExecute: enabled, IsExecuting: executing, Execute: (n: number) => of(n * 2) }); return null; }
  const container = window.document.createElement('div'); const root = createRoot(container as any);
  await act(async () => root.render(createElement(View)));
  assert.equal(await hook!.execute(3), 6);
  await act(async () => enabled.next(false));
  assert.equal(hook!.canExecute, false); await assert.rejects(hook!.Execute(3), /cannot execute/);
  await act(async () => root.unmount());
});

test('React RoutedViewHost resolves component types and follows navigation', async () => {
  class Page { constructor(public Name: string) {} }
  function PageView({ viewModel }: { viewModel: Page }) { return createElement('span', null, viewModel.Name); }
  const locator = new ViewLocator(); locator.Register(Page, () => PageView);
  const route = new BehaviorSubject<Page | null>(new Page('Home'));
  const container = window.document.createElement('div'); const root = createRoot(container as any);
  await act(async () => root.render(createElement(RoutedViewHost, { router: { CurrentViewModel: route }, viewLocator: locator, fallback: 'Empty' })));
  assert.equal(container.textContent, 'Home');
  await act(async () => route.next(new Page('Details'))); assert.equal(container.textContent, 'Details');
  await act(async () => route.next(null)); assert.equal(container.textContent, 'Empty');
  await act(async () => root.unmount()); assert.equal(route.observers.length, 0);
});

test('React command invocation gates two calls made in the same turn', async () => {
  let resolve!: (value: number) => void;
  const command = ReactiveCommand.CreateFromTask<number, number>(() => new Promise<number>(done => { resolve = done; }));
  let hook: ReturnType<typeof useReactiveCommand<number, number>>;
  function View() { hook = useReactiveCommand(command); return null; }
  const container = window.document.createElement('div'); const root = createRoot(container as any);
  await act(async () => root.render(createElement(View)));
  await act(async () => {
    const first = hook!.execute(1);
    await assert.rejects(hook!.execute(2), /cannot execute/);
    resolve(1); assert.equal(await first, 1);
  });
  await act(async () => root.unmount()); command.Dispose();
});
