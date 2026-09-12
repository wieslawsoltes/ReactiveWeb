import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Window } from 'happy-dom';
import { BehaviorSubject, firstValueFrom, of } from 'rxjs';
import { ReactiveObject } from '../dist/reactive-object.js';
import { ObservableCollection } from '../dist/collections.js';
import { SourceCache, SourceList } from '@wieslawsoltes/dynamicdataweb';
import { ViewModelActivator, WhenActivated } from '../dist/activation.js';
import { ViewLocator } from '../dist/services.js';
import { Interaction } from '../dist/interaction.js';
import { ConverterService, PropertyBindingHookRegistry } from '../dist/converters.js';
const window = new Window();
Object.assign(globalThis, { window, document: window.document, HTMLElement: window.HTMLElement, customElements: window.customElements, MutationObserver: window.MutationObserver });
const { Bind, OneWayBind, BindCommand, BindHtml, BindingConverters, ReactiveElement, RoutedViewHost, BindInteraction, BindCollection } = await import('../dist/html.js');
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


test('BindCollection preserves duplicate occurrence nodes through indexed moves and removals', () => {
  const duplicate = { Name: 'same' }, other = { Name: 'other' };
  const source = new SourceList([duplicate, duplicate, other]);
  const target = document.createElement('ul'); let disposed = 0;
  const binding = BindCollection(source, target as any, (item, index, lifetime) => {
    const row = document.createElement('li'); row.textContent = item.Name;
    lifetime.Add(() => disposed++); return row as any;
  });
  const [first, second, third] = [...target.children];
  source.Move(1, 2);
  assert.deepEqual([...target.children], [first, third, second]); assert.equal(disposed, 0);
  source.RemoveAt(0);
  assert.deepEqual([...target.children], [third, second]); assert.equal(disposed, 1);
  source.Add({ Name: 'new' }); assert.equal(target.children[1], second);
  binding.Dispose(); assert.equal(disposed, 4); assert.equal(target.childNodes.length, 0);
  source.Add({ Name: 'after disposal' }); assert.equal(target.children.length, 0);
  assert.equal(source.isDisposed, false); source.Dispose();
});

test('BindCollection retains keyed replacements and refreshes when an update callback is supplied', () => {
  const source = new SourceCache<{ Id: number; Name: string }, number>(item => item.Id);
  source.AddOrUpdate([{ Id: 1, Name: 'First' }, { Id: 2, Name: 'Second' }]);
  const target = document.createElement('ul'); let disposed = 0;
  const binding = BindCollection(source, target as any, (_item, _index, lifetime) => {
    lifetime.Add(() => disposed++); return document.createElement('li') as any;
  }, { keySelector: item => item.Id, update: (node, item, index) => { node.textContent = `${index}:${item.Name}`; } });
  const first = target.children[0], second = target.children[1];
  source.AddOrUpdate({ Id: 1, Name: 'Replacement' });
  assert.equal(target.children[0], first); assert.equal(first.textContent, '0:Replacement'); assert.equal(disposed, 0);
  source.Lookup(1).Value.Name = 'Refreshed'; source.RefreshKey(1);
  assert.equal(first.textContent, '0:Refreshed'); assert.equal(target.children[1], second);
  source.RemoveKey(1); assert.equal(target.children[0], second); assert.equal(second.textContent, '0:Second');
  binding.Dispose(); assert.equal(disposed, 2); source.Dispose();
});

test('BindCollection replaces sources and disposes old row property subscriptions exactly once', () => {
  const one = new ObservableCollection([new ReactiveObject({ Name: 'One' }) as any]);
  const two = new ObservableCollection([new ReactiveObject({ Name: 'Two' }) as any]);
  const target = document.createElement('ul'); let disposed = 0;
  const binding = BindCollection(one, target as any, (item, _index, lifetime) => {
    const row = document.createElement('li'); lifetime.Add(OneWayBind(item, 'Name', row as any)); lifetime.Add(() => disposed++); return row as any;
  });
  const detached = target.children[0]; binding.Source = two;
  assert.equal(disposed, 1); assert.equal(target.textContent, 'Two');
  one.GetAt(0).Name = 'Stale'; assert.equal(detached.textContent, 'One');
  one.Add(new ReactiveObject({ Name: 'Old addition' })); assert.equal(target.children.length, 1);
  two.GetAt(0).Name = 'Current'; assert.equal(target.textContent, 'Current');
  binding.Dispose(); binding.Dispose(); assert.equal(disposed, 2); assert.equal(one.IsDisposed, false);
  one.Dispose(); two.Dispose();
});

test('BindCollection preserves rows across reset, rebuilds immutable replacements without update, and keeps unmanaged content', () => {
  const a = { Id: 1, Name: 'A' }, b = { Id: 2, Name: 'B' };
  const source = new ObservableCollection([a, b]); const target = document.createElement('ul');
  const heading = document.createElement('li'); heading.textContent = 'Heading'; target.append(heading);
  const binding = BindCollection(source, target as any, item => { const row = document.createElement('li'); row.textContent = item.Name; return row as any; }, { keySelector: item => item.Id });
  const first = target.children[1], second = target.children[2];
  source.Reset([b, a]); assert.deepEqual([...target.children], [heading, second, first]);
  source.SetAt(1, { Id: 1, Name: 'Changed' }); assert.notEqual(target.children[2], first); assert.equal(target.children[2].textContent, 'Changed');
  binding.Dispose(); assert.deepEqual([...target.children], [heading]); source.Dispose();
});


test('BindCollection releases rows created before a renderer failure', () => {
  const source = new ObservableCollection(['first', 'fail']); const target = document.createElement('ul'); let disposed = 0;
  assert.throws(() => BindCollection(source, target as any, (item, _index, lifetime) => {
    lifetime.Add(() => disposed++); if (item === 'fail') throw new Error('renderer failed');
    return document.createElement('li') as any;
  }), /renderer failed/);
  assert.equal(disposed, 2); assert.equal(target.childNodes.length, 0); source.Dispose();
});


test('BindCollection queues source edits made while a row is being rendered', () => {
  const source = new ObservableCollection(['first']); const target = document.createElement('ul'); let disposed = 0;
  const binding = BindCollection(source, target as any, (item, _index, lifetime) => {
    const row = document.createElement('li'); row.textContent = item; lifetime.Add(() => disposed++);
    if (item === 'first') source.Add('second');
    return row as any;
  });
  assert.deepEqual([...target.children].map(node => node.textContent), ['first', 'second']);
  source.Move(0, 1); assert.deepEqual([...target.children].map(node => node.textContent), ['second', 'first']);
  binding.Dispose(); assert.equal(disposed, 2); source.Dispose();
});


test('BindCollection created during snapshot notification does not repeat the pending delta', () => {
  const source = new ObservableCollection(['a']); const target = document.createElement('ul');
  let binding: ReturnType<typeof BindCollection<string>> | undefined;
  const subscription = source.ItemsChanged.subscribe(items => {
    if (items.length === 2 && !binding) binding = BindCollection(source, target as any, item => {
      const row = document.createElement('li'); row.textContent = item; return row as any;
    });
  });
  source.Add('b'); assert.equal(target.textContent, 'ab'); assert.equal(target.children.length, 2);
  const first = target.children[0]; source.Add('c'); assert.equal(target.textContent, 'abc'); assert.equal(target.children[0], first);
  binding!.Dispose(); subscription.unsubscribe(); source.Dispose();
});
