import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Window } from 'happy-dom';
import { BehaviorSubject, firstValueFrom, of } from 'rxjs';
import { ReactiveObject } from '../dist/reactive-object.js';
import { ViewModelActivator, WhenActivated } from '../dist/activation.js';
import { ViewLocator } from '../dist/services.js';
import { Interaction } from '../dist/interaction.js';
import { ConverterService, PropertyBindingHookRegistry } from '../dist/converters.js';
const window = new Window();
Object.assign(globalThis, { window, document: window.document, HTMLElement: window.HTMLElement, customElements: window.customElements, MutationObserver: window.MutationObserver });
const { Bind, OneWayBind, BindCommand, BindHtml, BindingConverters, ReactiveElement, RoutedViewHost, BindInteraction } = await import('../dist/html.js');
const document = window.document;

test('two-way binding handles conversion, nested replacement, IME and disposal', () => {
  const vm = new ReactiveObject({ Child: new ReactiveObject({ Count: 3 }) }) as any;
  const input = document.createElement('input');
  const binding = Bind(vm, 'Child.Count', input as any, 'value', { converter: BindingConverters.Number });
  assert.equal(input.value, '3');
  input.value = '12'; input.dispatchEvent(new window.Event('input'));
  assert.equal(vm.Child.Count, 12);
  input.dispatchEvent(new window.Event('compositionstart'));
  input.value = '100'; input.dispatchEvent(new window.Event('input'));
  assert.equal(vm.Child.Count, 12);
  input.dispatchEvent(new window.Event('compositionend'));
  assert.equal(vm.Child.Count, 100);
  vm.Child = new ReactiveObject({ Count: 9 });
  assert.equal(input.value, '9');
  binding.Dispose();
  input.value = '24'; input.dispatchEvent(new window.Event('input'));
  assert.equal(vm.Child.Count, 9);
});

test('DOM output uses text and rejects prototype pollution paths', () => {
  const vm = new ReactiveObject({ Text: '<img src=x onerror=alert(1)>' });
  const output = document.createElement('div');
  const binding = OneWayBind(vm, 'Text', output as any);
  assert.equal(output.children.length, 0);
  assert.match(output.textContent!, /<img/);
  assert.throws(() => Bind(vm, '__proto__.polluted', output as any));
  binding.Dispose();
});

test('command binding gates events and releases completed execution subscriptions', () => {
  const enabled = new BehaviorSubject(true);
  let count = 0;
  const button = document.createElement('button');
  const binding = BindCommand({ CanExecute: enabled, Execute(value: number) { count += value; return of(count); } }, button as any, { parameter: 2 });
  for (let i = 0; i < 100; i++) button.click();
  assert.equal(count, 200);
  assert.equal(binding.Count, 2);
  enabled.next(false); button.click();
  assert.equal(count, 200);
  binding.Dispose(); enabled.next(true); button.click();
  assert.equal(count, 200);
});

test('declarative binding tracks added/removed nodes and attribute replacements', async () => {
  const vm = new ReactiveObject({ Name: 'Ada', Other: 'Grace' }) as any;
  const root = document.createElement('div');
  const binding = BindHtml(root as any, vm);
  const input = document.createElement('input'); input.setAttribute('data-rx-bind', 'Name'); root.append(input);
  await window.happyDOM.waitUntilComplete();
  assert.equal(input.value, 'Ada');
  input.setAttribute('data-rx-bind', 'Other');
  await window.happyDOM.waitUntilComplete();
  assert.equal(input.value, 'Grace');
  input.remove(); await window.happyDOM.waitUntilComplete();
  input.value = 'Old'; input.dispatchEvent(new window.Event('input'));
  assert.equal(vm.Other, 'Grace');
  binding.Dispose();
});

test('ReactiveElement releases and recreates activation when detached or rebound', () => {
  class Example extends ReactiveElement<any> {}
  window.customElements.define('test-reactive-view', Example as any);
  const first = new ReactiveObject({ Name: 'First' }) as any; first.Activator = new ViewModelActivator();
  const second = new ReactiveObject({ Name: 'Second' }) as any; second.Activator = new ViewModelActivator();
  let activated = 0, disposed = 0;
  const view = document.createElement('test-reactive-view') as any;
  view.innerHTML = '<span data-rx-text="Name"></span>';
  view.WhenActivated((lifetime: any) => { activated++; lifetime.Add(() => disposed++); });
  view.ViewModel = first; document.body.append(view);
  assert.equal(view.textContent, 'First'); assert.equal(first.Activator.ReferenceCount, 1);
  view.ViewModel = second;
  assert.equal(view.textContent, 'Second'); assert.equal(first.Activator.ReferenceCount, 0); assert.equal(second.Activator.ReferenceCount, 1);
  view.remove(); assert.equal(second.Activator.ReferenceCount, 0);
  document.body.append(view); assert.equal(second.Activator.ReferenceCount, 1);
  view.remove(); assert.equal(activated, 3); assert.equal(disposed, 3);
});

test('RoutedViewHost switches routers and contracts and detaches old views', () => {
  class Page { constructor(public Name: string) {} }
  const locator = new ViewLocator();
  locator.Register(Page, vm => { const element = document.createElement('p'); element.textContent = vm.Name; return element; });
  locator.Register(Page, vm => { const element = document.createElement('b'); element.textContent = `${vm.Name}!`; return element; }, 'bold');
  class Host extends RoutedViewHost {}
  window.customElements.define('test-route-host', Host as any);
  const host = document.createElement('test-route-host') as any;
  host.ViewLocator = locator;
  const first = new BehaviorSubject<Page | null>(new Page('one')), second = new BehaviorSubject<Page | null>(new Page('two'));
  host.Router = { CurrentViewModel: first }; document.body.append(host);
  assert.equal(host.textContent, 'one');
  host.Router = { CurrentViewModel: second }; first.next(new Page('stale'));
  assert.equal(host.textContent, 'two'); assert.equal(first.observers.length, 0);
  host.ViewContract = 'bold'; assert.equal(host.textContent, 'two!');
  host.DefaultContent = 'Empty'; second.next(null); assert.equal(host.textContent, 'Empty');
  host.remove(); assert.equal(second.observers.length, 0);
});

test('BindInteraction follows replaced interaction instances and disposes registrations', async () => {
  const original = new Interaction<string, string>();
  const vm = new ReactiveObject({ Prompt: original }) as any;
  const binding = BindInteraction<string, string>(vm, 'Prompt', context => context.SetOutput(`${context.Input}!`));
  assert.equal(await firstValueFrom(original.Handle('one')), 'one!');
  const replacement = new Interaction<string, string>(); vm.Prompt = replacement;
  await assert.rejects(firstValueFrom(original.Handle('old')));
  assert.equal(await firstValueFrom(replacement.Handle('two')), 'two!');
  binding.Dispose(); await assert.rejects(firstValueFrom(replacement.Handle('end')));
});

test('binding supports converter service tokens and a provider veto before subscription', () => {
  const vm = new ReactiveObject({ Count: 12 }) as any;
  const input = document.createElement('input');
  const service = new ConverterService();
  const binding = Bind(vm, 'Count', input as any, 'value', { conversionService: service, sourceType: Number, targetType: String });
  assert.equal(input.value, '12'); input.value = '25'; input.dispatchEvent(new window.Event('input')); assert.equal(vm.Count, 25);
  binding.Dispose();
  const hooks = new PropertyBindingHookRegistry(); hooks.Register({ ExecuteHook: () => false });
  const rejected = Bind(vm, 'Count', input as any, 'value', { bindingHooks: hooks });
  assert.equal(rejected.IsBound, false);
  input.value = '99'; input.dispatchEvent(new window.Event('input')); assert.equal(vm.Count, 25);
});

test('parent declarative bindings respect nested light-DOM view model boundaries', async () => {
  class NestedView extends ReactiveElement<any> {}
  window.customElements.define('test-nested-view', NestedView as any);
  const parent = document.createElement('div');
  const child = document.createElement('test-nested-view') as any;
  child.innerHTML = '<span data-rx-text="Name"></span>';
  const parentVm = new ReactiveObject({ Name: 'Parent' }) as any;
  const childVm = new ReactiveObject({ Name: 'Child' }) as any;
  child.ViewModel = childVm; parent.append(child); document.body.append(parent);
  const binding = BindHtml(parent as any, parentVm);
  await window.happyDOM.waitUntilComplete();
  assert.equal(child.textContent, 'Child');
  parentVm.Name = 'Changed parent'; assert.equal(child.textContent, 'Child');
  childVm.Name = 'Changed child'; assert.equal(child.textContent, 'Changed child');
  binding.Dispose(); parent.remove();
});

test('singleton views survive route A to B to A and are disposed by registration ownership', () => {
  class PageA extends ReactiveObject { constructor(name: string) { super({ Name: name }); } }
  class PageB {}
  class SingletonView extends ReactiveElement<any> {}
  class Host extends RoutedViewHost {}
  window.customElements.define('test-singleton-view', SingletonView as any);
  window.customElements.define('test-singleton-host', Host as any);
  const locator = new ViewLocator();
  let creations = 0;
  const registration = locator.RegisterSingleton(PageA, () => {
    creations++; const view = document.createElement('test-singleton-view') as any;
    view.innerHTML = '<span data-rx-text="Name"></span>'; return view;
  });
  locator.Register(PageB, () => document.createElement('p'));
  const route = new BehaviorSubject<object | null>(new PageA('First'));
  const host = document.createElement('test-singleton-host') as any;
  host.ViewLocator = locator; host.Router = { CurrentViewModel: route }; document.body.append(host);
  assert.equal(host.textContent, 'First');
  route.next(new PageB()); route.next(new PageA('Again'));
  assert.equal(host.textContent, 'Again'); assert.equal(creations, 1);
  const view = host.firstElementChild; assert.equal(view.Activator.IsActiveValue, true);
  host.remove(); assert.equal(view.Activator.IsActiveValue, false);
  registration.Dispose(); assert.throws(() => view.Activator.Activate(), /disposed/);
});
