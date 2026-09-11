import { BehaviorSubject, Subject, Observable, Subscription, interval, timer, map, take, firstValueFrom } from 'rxjs';
import { createElement, StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import {
  ReactiveObject, DefineReactiveProperty, WhenAnyValue, ToProperty, ReactiveCommand,
  Interaction, CompositeDisposable, Disposable, ViewModelActivator, WhenActivated,
  RoutingState, ViewLocator, MessageBus, ServiceLocator, ObservableCollection,
  BindableDerivedList, LocalStorageSuspensionDriver, AutoPersist, SuspensionHost,
  ReactiveValidationObject, ValidationRule, defineViewModel, reactiveProperty, RxApp
} from '../src/index.js';
import { Bind, OneWayBind, BindCommand, BindValidation, ReactiveElement, RoutedViewHost, RegisterReactiveElements } from '../src/html.js';
import { useReactiveObject, useReactiveCommand, useWhenActivated } from '../src/react.js';

const $ = <T extends HTMLElement = HTMLElement>(id: string) => document.getElementById(id) as T;
RegisterReactiveElements();
const lab = $('lab');
let scope = new CompositeDisposable();
let eventCount = 0;
let theme = localStorage.getItem('reactiveweb.theme') || 'light';
document.documentElement.dataset.theme = theme;
$('theme-toggle').onclick = () => { theme = theme === 'light' ? 'dark' : 'light'; document.documentElement.dataset.theme = theme; localStorage.setItem('reactiveweb.theme', theme); };
function log(kind: string, value: unknown) {
  eventCount++; $('event-count').textContent = String(eventCount);
  const row = document.createElement('div'); row.className = 'event-row';
  const time = document.createElement('time'); time.textContent = new Date().toLocaleTimeString('en-GB', { hour12: false });
  const name = document.createElement('strong'); name.textContent = kind;
  const data = document.createElement('span'); data.textContent = typeof value === 'string' ? value : JSON.stringify(value);
  row.append(time, name, data); $('events').prepend(row);
  while ($('events').children.length > 80) $('events').lastElementChild?.remove();
}
function inspect(value: unknown, name = 'ViewModel') { $('model-name').textContent = name; $('state').textContent = JSON.stringify(value, null, 2); }
function code(value: string) { $('source-code').textContent = value.trim(); }
function on(id: string, event: string, handler: EventListener) {
  const element = $(id); element.addEventListener(event, handler); scope.Add(Disposable.Create(() => element.removeEventListener(event, handler)));
}
function model(values: Record<string, unknown>): ReactiveObject & Record<string, any> {
  const value = new ReactiveObject() as ReactiveObject & Record<string, any>;
  for (const [name, initial] of Object.entries(values)) DefineReactiveProperty(value, name, initial);
  scope.Add(value); return value;
}
function watch(vm: ReactiveObject & Record<string, any>, names: string[], title: string) {
  const update = () => inspect(Object.fromEntries(names.map(name => [name, vm[name]])), title);
  scope.Add(vm.Changed.subscribe(event => { log(event.PropertyName + ' changed', event.Value); update(); })); update();
}
function commandEvents(command: ReactiveCommand<any, any>, name = 'Command') {
  scope.Add(command); scope.Add(command.ThrownExceptions.subscribe(error => log(name + '.error', String(error))));
  scope.Add(command.subscribe(value => log(name + '.result', value)));
  scope.Add(command.IsExecuting.subscribe(value => log(name + '.IsExecuting', value)));
}
$('clear-events').onclick = () => { $('events').replaceChildren(); eventCount = 0; $('event-count').textContent = '0'; };

const profileSource = `class ProfileViewModel extends ReactiveObject {
  @Reactive accessor FirstName = 'Alex';
  @Reactive accessor LastName = 'Morgan';

  FullName = ToProperty(
    WhenAnyValue(this, 'FirstName', 'LastName',
      (first, last) => first + ' ' + last),
    this, 'DisplayName', { initialValue: '' }
  );
  Save = ReactiveCommand.CreateFromTask(async () => saveProfile(this),
    WhenAnyValue(this, 'FirstName', name => name.trim().length > 0));
}

DisposeWith(Bind(vm, 'FirstName', firstNameInput, 'value'), lifetime);
OneWayBind(vm, 'DisplayName', preview, 'textContent');
BindCommand(vm.Save, saveButton);`;

function profile() {
  lab.innerHTML = `<div class="form-grid"><div class="field"><label for="first-name">First name</label><input id="first-name" autocomplete="given-name"></div><div class="field"><label for="last-name">Last name</label><input id="last-name" autocomplete="family-name"></div></div><div class="field"><label for="email">Email address</label><input id="email" type="email" autocomplete="email"><small>Input changes flow straight into your view model.</small></div><div class="live-preview"><div id="initials" class="avatar"></div><div><strong id="full-name"></strong><small id="email-preview"></small></div></div><div class="button-row"><button id="save" class="primary">Save profile</button><button id="reset" class="secondary">Reset</button><span class="status" id="save-status">Ready for changes</span></div>`;
  const vm = model({ FirstName: 'Alex', LastName: 'Morgan', Email: 'alex.morgan@example.com' });
  scope.Add(ToProperty(WhenAnyValue(vm, 'FirstName', 'LastName', (a: string, b: string) => `${a} ${b}`.trim()), vm, 'DisplayName', { initialValue: '' }));
  for (const [property, id] of [['FirstName','first-name'],['LastName','last-name'],['Email','email']]) scope.Add(Bind(vm, property, $(id), 'value'));
  scope.Add(OneWayBind(vm, 'DisplayName', $('full-name'), 'textContent'));
  scope.Add(OneWayBind(vm, 'Email', $('email-preview'), 'textContent'));
  scope.Add(WhenAnyValue(vm,'FirstName','LastName',(a: string,b: string) => `${a[0]||''}${b[0]||''}`.toUpperCase()).subscribe(value => { $('initials').textContent = value as string; }));
  const save = ReactiveCommand.CreateFromObservable(() => timer(450).pipe(map(() => ({ name: vm.DisplayName, email: vm.Email }))), WhenAnyValue(vm,'FirstName',(name: string) => !!name.trim()) as Observable<boolean>);
  commandEvents(save, 'Save'); scope.Add(BindCommand(save, $('save')));
  scope.Add(save.IsExecuting.subscribe(value => { if (value) $('save-status').textContent = 'Saving profile…'; }));
  scope.Add(save.subscribe(() => { $('save-status').textContent = 'Profile saved in this session'; }));
  on('reset','click',() => { const batch = vm.DelayChangeNotifications(); vm.FirstName='Alex'; vm.LastName='Morgan'; vm.Email='alex.morgan@example.com'; batch.Dispose(); });
  watch(vm, ['FirstName','LastName','Email','DisplayName'], 'ProfileViewModel'); code(profileSource);
  log('WhenAnyValue subscribed', 'FirstName + LastName → DisplayName');
}
function properties() {
  lab.innerHTML = `<div class="tags"><span class="tag">Nested observation</span><span class="tag">Batch notifications</span><span class="tag">Distinct changes</span></div><div class="field"><label for="city">Customer.Address.City</label><input id="city"></div><div class="live-preview"><div class="avatar">↳</div><div><strong id="city-preview"></strong><small>A nested subscription follows replaced objects.</small></div></div><div class="button-row"><button class="primary" id="replace">Replace address</button><button class="secondary" id="batch">Batch 3 writes</button><button class="secondary" id="same">Write same value</button></div><p class="note">A replaced Address detaches the old subscription. A delayed batch coalesces repeated writes to the same property.</p>`;
  const address = model({ City:'Warsaw' }); const vm=model({Address:address});
  scope.Add(Bind(vm,'Address.City',$('city'),'value'));
  scope.Add(WhenAnyValue(vm,'Address.City').subscribe(city => { $('city-preview').textContent = String(city); inspect({ Address:{ City:city } },'CustomerViewModel'); log('Address.City', city); }));
  on('replace','click',()=>{vm.Address=model({City:vm.Address.City==='Warsaw'?'Kraków':'Warsaw'});log('Address replaced','Nested subscription rewired');});
  on('batch','click',()=>{const batch=vm.Address.DelayChangeNotifications();vm.Address.City='Gdańsk';vm.Address.City='Poznań';vm.Address.City='Wrocław';batch.Dispose();log('Batch complete','Three writes, one property notification');});
  on('same','click',()=>{vm.Address.City=vm.Address.City;log('Equal write','No property event emitted');});
  code(`WhenAnyValue(customer, 'Address.City').subscribe(renderCity);
customer.Address = new AddressViewModel('Kraków'); // automatically resubscribes
const batch = customer.Address.DelayChangeNotifications();
customer.Address.City = 'Gdańsk';
customer.Address.City = 'Wrocław';
batch.Dispose(); // one final City notification`);
}
function commands() {
  lab.innerHTML = `<div class="field"><label for="job">Job name</label><input id="job" value="Compile view models"></div><label class="switch-row"><input id="allowed" type="checkbox" checked>Allow execution</label><div class="metric" id="progress">0%</div><div class="button-row"><button id="run" class="primary">Run command</button><button id="cancel" class="secondary" disabled>Cancel</button><button id="fail" class="secondary">Test exception</button></div><p class="note">The UI respects CanExecute. Execute() is an explicit cold observable; each subscription starts its own execution. Cancel releases the observable subscription.</p>`;
  const allowed = new BehaviorSubject(true); scope.Add(Disposable.Create(()=>allowed.complete()));
  const run=ReactiveCommand.CreateFromObservable<string,number>(()=>interval(140).pipe(take(10),map(i=>(i+1)*10)),allowed);
  commandEvents(run,'Run'); let execution:Subscription|undefined;
  scope.Add(run.CanExecute.subscribe(v=>{$<HTMLButtonElement>('run').disabled=!v;}));
  scope.Add(run.IsExecuting.subscribe(v=>{$<HTMLButtonElement>('cancel').disabled=!v;inspect({CanExecute:run.CanExecuteValue,IsExecuting:v},'ReactiveCommand');}));
  scope.Add(run.subscribe(v=>{$('progress').textContent=`${v}%`;}));
  on('allowed','change',()=>allowed.next($<HTMLInputElement>('allowed').checked));
  on('run','click',()=>{execution=run.Execute($<HTMLInputElement>('job').value).subscribe();scope.Add(execution);});
  on('cancel','click',()=>{execution?.unsubscribe();log('Execution cancelled','Subscription disposed');});
  const fail=ReactiveCommand.Create<void,void>(()=>{throw new Error('A deliberate failure, observed safely.');});commandEvents(fail,'Failure');scope.Add(BindCommand(fail,$('fail'),{onError:()=>{}}));
  code(`const command = ReactiveCommand.CreateFromObservable(
  () => interval(140).pipe(take(10), map(i => (i + 1) * 10)), canRun$);
command.IsExecuting.subscribe(showProgress);
command.ThrownExceptions.subscribe(showError);
const execution = command.Execute().subscribe(renderResult);
execution.unsubscribe(); // releases observable work
// CreateFromTask((input, signal) => fetch(url, { signal })) supports abort.`);
}
class CounterElement extends ReactiveElement<any> {
  constructor(){super();this.attachShadow({mode:'open'}).innerHTML=`<style>:host{display:block;padding:18px;background:#edf4ff;border:1px solid #cdddff;border-radius:8px;color:#17334f;font:14px system-ui}button{background:#1766ea;border:0;border-radius:5px;color:white;padding:8px 12px;cursor:pointer;font:inherit}strong{display:inline-block;min-width:40px;margin-right:16px;font-size:26px}</style><strong data-rx-text="Count"></strong><button data-rx-command="Increment">Increment in component</button>`;}
}
customElements.define('demo-counter',CounterElement);
function bindings(){
  lab.innerHTML=`<div class="field"><label for="counter-input">Shared Count</label><input id="counter-input" type="number"></div><div id="component-slot"></div><div class="button-row"><button id="toggle-component" class="secondary">Detach component</button></div><p class="note">This is a real custom element with a shadow root. Declarative data-rx bindings activate when it connects and dispose when it disconnects.</p>`;
  const vm=model({Count:2});vm.Increment=ReactiveCommand.Create(()=>++vm.Count);commandEvents(vm.Increment,'Increment');
  scope.Add(Bind(vm,'Count',$('counter-input'),'value',{convert:(v)=>String(v),convertBack:(v)=>Number(v)}));
  const component=document.createElement('demo-counter') as CounterElement;component.ViewModel=vm;$('component-slot').append(component);scope.Add(Disposable.Create(()=>component.remove()));
  on('toggle-component','click',()=>{const connected=component.isConnected;if(connected)component.remove();else $('component-slot').append(component);$('toggle-component').textContent=connected?'Attach component':'Detach component';log('Component lifecycle',connected?'Disconnected; subscriptions disposed':'Connected; bindings active');});
  watch(vm,['Count'],'CounterViewModel');code(`class CounterView extends ReactiveElement<CounterViewModel> {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' }).innerHTML =
      '<strong data-rx-text="Count"></strong>' +
      '<button data-rx-command="Increment">Increment</button>';
  }
}
customElements.define('counter-view', CounterView);
element.ViewModel = vm; // rebinds on replacement; releases on disconnect`);
}
function activation(){
  lab.innerHTML=`<div class="tags"><span class="tag">WhenActivated</span><span class="tag">CompositeDisposable</span><span class="tag">Reference counting</span></div><p>The timer belongs to the current activation scope.</p><div class="metric" id="ticks">0</div><div class="button-row"><button id="activate" class="primary">Activate</button><button id="deactivate" class="secondary" disabled>Deactivate</button><button id="lease" class="secondary">Add second lease</button></div><p class="note">The final lease release disposes the timer. A new activation creates a fresh subscription.</p>`;
  const vm={Activator:new ViewModelActivator()};scope.Add(vm.Activator);let lease:ReturnType<ViewModelActivator['Activate']>|undefined;let second:typeof lease;let ticks=0;
  const update=()=>inspect({IsActive:vm.Activator.IsActiveValue,ReferenceCount:vm.Activator.ReferenceCount,Ticks:ticks},'ActivatableViewModel');
  scope.Add(WhenActivated(vm,d=>{ticks=0;log('WhenActivated','Subscription created');d.Add(interval(300).subscribe(()=>{$('ticks').textContent=String(++ticks);update();}));d.Add(Disposable.Create(()=>log('Scope disposed','Timer unsubscribed')));}));
  on('activate','click',()=>{lease??=vm.Activator.Activate();$<HTMLButtonElement>('activate').disabled=true;$<HTMLButtonElement>('deactivate').disabled=false;update();});
  on('deactivate','click',()=>{lease?.Dispose();lease=undefined;$<HTMLButtonElement>('activate').disabled=false;$<HTMLButtonElement>('deactivate').disabled=true;update();});
  on('lease','click',()=>{if(second){second.Dispose();second=undefined;}else second=vm.Activator.Activate();$('lease').textContent=second?'Release second lease':'Add second lease';update();});update();
  code(`WhenActivated(vm, disposables => {
  disposables.Add(interval(300).subscribe(renderTick));
});
const first = vm.Activator.Activate();
const second = vm.Activator.Activate();
first.Dispose(); // remains active
second.Dispose(); // final release cleans up the activation scope`);
}
function routing(){
  lab.innerHTML=`<div class="tags"><span class="tag">IScreen</span><span class="tag">IRoutableViewModel</span><span class="tag">ViewLocator</span></div><div class="button-row"><button id="navigate" class="primary">Open details</button><button id="back" class="secondary">Go back</button><button id="route-reset" class="secondary">Reset navigation</button></div><div class="route-preview" id="route-host"></div><p class="note">The view host resolves a real DOM view from the current view model. NavigateBack becomes available when the stack contains two or more items.</p>`;
  const router=new RoutingState();scope.Add(router);const screen={Router:router};
  class Page{constructor(public UrlPathSegment:string,public Title:string){}HostScreen=screen;}
  const locator=new ViewLocator();scope.Add(locator.Register(Page,vm=>{const node=document.createElement('div');const h=document.createElement('h2');h.textContent=vm.Title;const p=document.createElement('p');p.textContent=`Route: /${vm.UrlPathSegment}`;node.append(h,p);return node;}));
  const host=document.createElement('reactive-routed-view-host') as RoutedViewHost;host.ViewLocator=locator;host.Router=router;$('route-host').append(host);scope.Add(Disposable.Create(()=>host.remove()));
  scope.Add(router.CurrentViewModel.subscribe(vm=>{inspect({NavigationStack:router.NavigationStack.Items.map(x=>x.UrlPathSegment),CurrentViewModel:vm?.UrlPathSegment,CanGoBack:router.NavigationStack.Count>1},'RoutingState');log('CurrentViewModel',vm?.UrlPathSegment??null);}));
  scope.Add(BindCommand(router.NavigateBack,$('back')));
  on('navigate','click',()=>router.Navigate.Execute(new Page(`details-${router.NavigationStack.Count}`,'Project details')).subscribe());
  on('route-reset','click',()=>router.NavigateAndReset.Execute(new Page('home','Workspace home')).subscribe());router.Navigate.Execute(new Page('home','Workspace home')).subscribe();
  code(`const screen = { Router: new RoutingState() };
ViewLocator.Current.Register(DetailsViewModel, vm => new DetailsView(vm));
host.Router = screen.Router;
screen.Router.Navigate.Execute(detailsViewModel).subscribe();
BindCommand(screen.Router.NavigateBack, backButton);`);
}
function interactions(){
  lab.innerHTML=`<div class="tags"><span class="tag">Interaction&lt;Input, Output&gt;</span><span class="tag">Newest handler first</span></div><p>The view model requests a decision without knowing how the view presents it.</p><div class="live-preview" style="margin-top:20px"><div class="avatar">?</div><div><strong id="decision">Waiting for a request</strong><small>Handled by an HTML dialog in the view.</small></div></div><button class="primary" id="ask">Request confirmation</button><p class="note">Handlers are disposable. If no handler supplies a result, Handle emits an UnhandledInteractionError.</p>`;
  const interaction=new Interaction<string,boolean>();scope.Add(interaction);
  scope.Add(interaction.RegisterHandler(context=>new Observable<void>(subscriber=>{
    const dialog=$<HTMLDialogElement>('interaction-dialog');$('dialog-message').textContent=context.Input;
    const close=()=>{context.SetOutput(dialog.returnValue==='confirm');subscriber.complete();};dialog.addEventListener('close',close);dialog.showModal();
    return()=>{dialog.removeEventListener('close',close);if(dialog.open)dialog.close();};
  })));
  inspect({Input:null,Output:null,IsHandled:false},'Interaction');
  on('ask','click',()=>{const input='Apply the pending changes to this example?';log('Interaction.Handle',input);scope.Add(interaction.Handle(input).subscribe(result=>{$('decision').textContent=result?'Changes confirmed':'Changes cancelled';inspect({Input:input,Output:result,IsHandled:true},'Interaction');log('Interaction result',result);}));});
  code(`const Confirm = new Interaction<string, boolean>();
// The view owns the handler and its lifetime.
Confirm.RegisterHandler(async context => {
  context.SetOutput(await showDialog(context.Input));
});
// The view model only knows its input and result contracts.
Confirm.Handle('Apply changes?').subscribe(confirmed => { /* ... */ });`);
}
function validation(){
  lab.innerHTML=`<div class="field"><label for="username">User name</label><input id="username" autocomplete="off"><small>At least 3 characters. “admin” is reserved.</small></div><div id="username-errors" class="error" role="status"></div><div class="field"><label for="age">Age</label><input id="age" type="number"></div><div id="age-errors" class="error" role="status"></div><button class="primary" id="submit-validation">Submit form</button><p class="note">The asynchronous user-name rule cancels obsolete observable requests. Pending validation keeps the submit command disabled.</p>`;
  const vm=new ReactiveValidationObject() as ReactiveValidationObject & {Username:string;Age:number};scope.Add(vm);DefineReactiveProperty(vm,'Username','alex');DefineReactiveProperty(vm,'Age',28);
  scope.Add(ValidationRule<string>(vm,'Username',value=>timer(280).pipe(map(()=>value.trim().length<3?'Use at least 3 characters.':value.toLowerCase()==='admin'?'That user name is reserved.':true))));
  scope.Add(ValidationRule<number>(vm,'Age',age=>age>=18&&age<=120,'Age must be between 18 and 120.'));
  scope.Add(Bind(vm,'Username',$('username'),'value'));scope.Add(Bind(vm,'Age',$('age'),'value',{convertBack:v=>Number(v)}));
  scope.Add(BindValidation(vm,$('username-errors'),'Username'));scope.Add(BindValidation(vm,$('age-errors'),'Age'));
  const submit=ReactiveCommand.Create(()=>({Username:vm.Username,Age:vm.Age}),vm.ValidationContext.IsValid);commandEvents(submit,'Submit');scope.Add(BindCommand(submit,$('submit-validation')));
  scope.Add(vm.ValidationContext.ValidationStatusChange.subscribe(state=>{inspect({Username:vm.Username,Age:vm.Age,...state},'ValidationContext');log('ValidationStatus',state);}));
  code(`ValidationRule(vm, 'Username', value =>
  checkAvailability$(value)); // old subscriptions are cancelled
ValidationRule(vm, 'Age', age => age >= 18, 'Must be 18 or older.');
const Submit = ReactiveCommand.Create(save, vm.ValidationContext.IsValid);
BindValidation(vm, errorLabel, 'Username');
BindCommand(Submit, submitButton);`);
}
function collections(){
  lab.innerHTML=`<div class="field"><label for="task-name">Task name</label><div class="button-row"><input id="task-name" placeholder="Add a task" style="flex:1;min-width:120px"><button id="add-task" class="primary">Add task</button></div></div><label class="switch-row"><input id="only-active" type="checkbox">Show incomplete tasks only</label><ul id="tasks" class="list"></ul><div class="button-row"><button id="batch-tasks" class="secondary">Add 3 in one batch</button><button id="sort-tasks" class="secondary">Sort A → Z</button></div>`;
  const tasks=new ObservableCollection<any>([model({Title:'Create a view model',Done:true}),model({Title:'Connect a reactive command',Done:false}),model({Title:'Ship a web component',Done:false})]);scope.Add(tasks);
  const derived=new BindableDerivedList(tasks);scope.Add(derived);
  const render=()=>{const list=$('tasks');list.replaceChildren();for(const task of derived.Items){const li=document.createElement('li');const label=document.createElement('label');const check=document.createElement('input');check.type='checkbox';check.checked=task.Done;check.onchange=()=>{task.Done=check.checked;render();};const text=document.createElement('span');text.textContent=task.Title;text.className=task.Done?'completed':'';label.append(check,text);const remove=document.createElement('button');remove.textContent='×';remove.setAttribute('aria-label',`Remove ${task.Title}`);remove.onclick=()=>tasks.Remove(task);li.append(label,remove);list.append(li);}inspect({SourceCount:tasks.Count,VisibleCount:derived.Count,Items:tasks.Items.map(x=>({Title:x.Title,Done:x.Done}))},'ObservableCollection');};
  scope.Add(derived.ItemsChanged.subscribe(render));scope.Add(tasks.CollectionChanged.subscribe(changes=>log('CollectionChanged',changes.map(x=>({Reason:x.Reason,Count:x.Items.length})))));
  on('add-task','click',()=>{const input=$<HTMLInputElement>('task-name');if(input.value.trim()){tasks.Add(model({Title:input.value.trim(),Done:false}));input.value='';}});
  on('only-active','change',()=>derived.SetFilter($<HTMLInputElement>('only-active').checked?x=>!x.Done:undefined));
  on('sort-tasks','click',()=>derived.SetComparer((a,b)=>a.Title.localeCompare(b.Title)));
  on('batch-tasks','click',()=>tasks.Edit(list=>{for(const title of ['Write tests','Review lifecycle','Publish package'])list.Add(model({Title:title,Done:false}));}));
  code(`const tasks = new ObservableCollection<TaskViewModel>();
const active = new BindableDerivedList(tasks, {
  filter: task => !task.Done,
  comparer: (a, b) => a.Title.localeCompare(b.Title)
});
tasks.Edit(list => { list.Add(first); list.Add(second); });
active.ItemsChanged.subscribe(renderTasks);
// Live item Changed streams update filter and sort results.`);
}
function persistence(){
  lab.innerHTML=`<div class="field"><label for="draft">Draft text</label><textarea id="draft" rows="3" style="height:90px;resize:vertical"></textarea><small>Saved to this browser after 300 ms of inactivity.</small></div><div class="button-row"><button id="flush" class="primary">Flush now</button><button id="restore" class="secondary">Restore saved</button><button id="forget" class="secondary">Clear saved state</button></div><p id="persist-status" class="note">Edit the draft, navigate away, and return. The saved value is restored.</p>`;
  const driver=new LocalStorageSuspensionDriver<{Draft:string}>('reactiveweb.demo.draft');const saved=driver.LoadState();const vm=model({Draft:saved?.Draft??'A reactive draft that survives a page refresh.'});
  scope.Add(Bind(vm,'Draft',$('draft'),'value'));
  const persistence=AutoPersist(vm,()=>{driver.SaveState({Draft:vm.Draft});$('persist-status').textContent='Saved at '+new Date().toLocaleTimeString();log('SaveState',{Draft:vm.Draft});},{throttleMs:300,onError:error=>log('Persistence error',String(error))});scope.Add(persistence);
  on('flush','click',()=>{void persistence.Flush();});on('restore','click',()=>{vm.Draft=driver.LoadState()?.Draft??'';log('LoadState','Saved state restored');});on('forget','click',()=>{driver.InvalidateState();$('persist-status').textContent='Saved state cleared. Editing creates a new saved draft.';log('InvalidateState','Removed persisted key');});
  watch(vm,['Draft'],'DraftViewModel');code(`const driver = new LocalStorageSuspensionDriver('my-app.draft');
const persistence = AutoPersist(vm,
  () => driver.SaveState({ Draft: vm.Draft }),
  { throttleMs: 300, onError: showError });
await persistence.Flush();
const restored = driver.LoadState();
// SuspensionHost adds launch, resume, persist and invalidate streams.`);
}
function messaging(){
  lab.innerHTML=`<div class="field"><label for="message">Notification message</label><input id="message" value="View model updated"></div><div class="button-row"><button id="send-message" class="primary">Send message</button><button id="latest" class="secondary">Listen to latest</button><button id="service" class="secondary">Resolve service</button></div><div class="live-preview" style="margin-top:22px"><div class="avatar">↔</div><div><strong id="received">Listening on “notifications”</strong><small>The sender and receiver share an explicit token.</small></div></div><p class="note">ServiceLocator supports constant, transient and lazy singleton services with optional contracts. MessageBus supports live and latest-value channels.</p>`;
  const bus=new MessageBus();scope.Add(bus);const token=Symbol('notifications');let count=0;let instances=0;const services=new ServiceLocator();scope.Add(services.RegisterLazySingleton(()=>({Id:++instances,Name:'NotificationService'}),'notification-service'));
  scope.Add(bus.Listen<string>(token).subscribe(message=>{$('received').textContent=message;inspect({LastMessage:message,Received:++count,ServiceInstances:instances},'MessageBus');log('Listen',message);}));
  on('send-message','click',()=>bus.SendMessage($<HTMLInputElement>('message').value,token));
  on('latest','click',()=>scope.Add(bus.ListenIncludeLatest<string>(token).pipe(take(1)).subscribe(value=>log('ListenIncludeLatest',value))));
  on('service','click',()=>{const service=services.GetRequiredService('notification-service');inspect(service,'LazySingleton');log('GetService',service);});inspect({Channel:'notifications',Received:0,ServiceInstances:0},'MessageBus');
  code(`const token = Symbol('notifications');
bus.Listen<string>(token).subscribe(showNotification);
bus.SendMessage('View model updated', token);
bus.ListenIncludeLatest<string>(token).subscribe(showMostRecent);

Locator.CurrentMutable.RegisterLazySingleton(
  () => new NotificationService(), NotificationService);
Locator.Current.GetRequiredService(NotificationService);`);
}
function generation(){
  lab.innerHTML=`<div class="tags"><span class="tag">Standard decorators</span><span class="tag">Generated accessors</span><span class="tag">No eval</span></div><div class="form-grid"><div class="field"><label for="unit-price">Unit price</label><input id="unit-price" type="number" min="0" step="0.5"></div><div class="field"><label for="quantity">Quantity</label><input id="quantity" type="number" min="1"></div></div><div class="live-preview"><div class="avatar">∑</div><div><strong id="total"></strong><small>Computed from a schema-generated view model.</small></div></div><div class="code-small">npx reactiveweb-generate invoice.json --out Invoice.ts</div><p class="note">The build-time generator emits readable TypeScript or JavaScript. Standard @Reactive accessor decorators are another option for class-based applications.</p>`;
  const Invoice=defineViewModel({name:'InvoiceViewModel',properties:{UnitPrice:reactiveProperty(12.5),Quantity:reactiveProperty(3)},computed:{Total:{source:(vm:any)=>WhenAnyValue(vm,'UnitPrice','Quantity',(p:number,q:number)=>p*q),initialValue:0}}});
  const vm=new Invoice() as any;scope.Add(vm);
  scope.Add(Bind(vm,'UnitPrice',$('unit-price'),'value',{convertBack:v=>Number(v)}));scope.Add(Bind(vm,'Quantity',$('quantity'),'value',{convertBack:v=>Number(v)}));
  scope.Add(OneWayBind(vm,'Total',$('total'),'textContent',{convert:v=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(Number(v))}));
  watch(vm,['UnitPrice','Quantity','Total'],'GeneratedInvoiceViewModel');code(`class Invoice extends ReactiveObject {
  @Reactive accessor UnitPrice = 12.5;
  @Reactive accessor Quantity = 3;
}

// Buildless schema factory creates ordinary property descriptors.
const Invoice = defineViewModel({
  properties: { UnitPrice: reactiveProperty(12.5), Quantity: reactiveProperty(3) },
  computed: { Total: {
    source: vm => WhenAnyValue(vm, 'UnitPrice', 'Quantity', (p, q) => p * q),
    initialValue: 0
  }}
});`);
}
function react(){
  lab.innerHTML=`<div class="tags"><span class="tag">React 19</span><span class="tag">useSyncExternalStore</span><span class="tag">StrictMode</span></div><div id="react-root"></div><p class="note">This panel is rendered by React. useReactiveObject tracks property changes; useReactiveCommand binds command state; useWhenActivated owns the activation lease.</p>`;
  const vm=model({Count:0});vm.Activator=new ViewModelActivator();scope.Add(vm.Activator);vm.Increment=ReactiveCommand.Create(()=>++vm.Count);commandEvents(vm.Increment,'Increment');
  scope.Add(WhenActivated(vm as any,d=>{log('React activated','Effect owns a lease');d.Add(Disposable.Create(()=>log('React deactivated','Effect cleanup released lease')));}));
  function Counter(){const current=useReactiveObject(vm);const command=useReactiveCommand(vm.Increment);useWhenActivated(vm as any);return createElement('div',null,createElement('p',null,'A shared view model, rendered through React.'),createElement('div',{className:'metric','data-testid':'react-count'},String(current.Count)),createElement('button',{className:'primary',id:'react-increment',disabled:!command.canExecute,onClick:()=>void command.execute(undefined)},'Increment with React'));}
  const root=createRoot($('react-root'));root.render(createElement(StrictMode,null,createElement(Counter)));scope.Add(Disposable.Create(()=>root.unmount()));watch(vm,['Count'],'ReactCounterViewModel');
  code(`function Counter({ viewModel }) {
  const vm = useReactiveObject(viewModel);
  const command = useReactiveCommand(vm.Increment);
  useWhenActivated(vm);
  return <button disabled={!command.canExecute}
    onClick={() => command.execute(undefined)}>
    Count: {vm.Count}
  </button>;
}
// React remains an optional peer dependency of the /react entry point.`);
}
const examples:Record<string,{title:string;icon:string;heading:string;description:string;lab:string;meta:string;render:()=>void}>={
  overview:{title:'Overview',icon:'▦',heading:'State that connects.',description:'Edit a view model. Follow the change from property to command to view.',lab:'Profile editor',meta:'TWO-WAY BINDING',render:profile},
  properties:{title:'Reactive objects',icon:'◇',heading:'Every property, observable.',description:'Follow nested values, replace objects, and batch notifications.',lab:'Nested property explorer',meta:'REACTIVEOBJECT',render:properties},
  commands:{title:'Commands',icon:'ϟ',heading:'Actions with a lifecycle.',description:'Run, cancel, gate, and observe asynchronous work.',lab:'Command runner',meta:'REACTIVECOMMAND',render:commands},
  bindings:{title:'HTML & components',icon:'⌘',heading:'Your markup. Reactive.',description:'Bind HTML directly or package a view as a reusable custom element.',lab:'Shared counter component',meta:'REACTIVEELEMENT',render:bindings},
  activation:{title:'Activation',icon:'◉',heading:'Subscriptions with a home.',description:'Own resources for exactly as long as a view needs them.',lab:'Activation scope',meta:'WHENACTIVATED',render:activation},
  routing:{title:'Routing',icon:'↳',heading:'Navigate by view model.',description:'Push views, go back, and resolve the right component for each route.',lab:'Navigation stack',meta:'ROUTEDVIEWHOST',render:routing},
  interactions:{title:'Interactions',icon:'⇄',heading:'Ask the view. Keep the model.',description:'Request a user decision through a typed interaction contract.',lab:'Confirmation workflow',meta:'INTERACTION',render:interactions},
  validation:{title:'Validation',icon:'✓',heading:'Valid state, ready to act.',description:'Compose synchronous and asynchronous validation with command gating.',lab:'Account form',meta:'VALIDATIONCONTEXT',render:validation},
  collections:{title:'Collections',icon:'☷',heading:'Collections that stay in sync.',description:'Batch mutations, observe item changes, and derive filtered lists.',lab:'Task collection',meta:'OBSERVABLECOLLECTION',render:collections},
  persistence:{title:'Persistence',icon:'▣',heading:'Keep the state that matters.',description:'Persist reactive changes through a pluggable state driver.',lab:'Persistent draft',meta:'AUTOPERSIST',render:persistence},
  messaging:{title:'Services & messages',icon:'↔',heading:'Connect without coupling.',description:'Resolve services and exchange typed messages across view models.',lab:'Message channel',meta:'MESSAGEBUS + LOCATOR',render:messaging},
  generation:{title:'Property generation',icon:'{ }',heading:'Less boilerplate. Same patterns.',description:'Generate ordinary accessors at build time or define a model from a schema.',lab:'Generated invoice',meta:'DEFINEVIEWMODEL',render:generation},
  react:{title:'React integration',icon:'⚛',heading:'Reactive models meet React.',description:'Use observable state and command lifecycles in familiar React components.',lab:'React counter',meta:'LIVE REACT COMPONENT',render:react}
};
for(const [id,example] of Object.entries(examples)){const link=document.createElement('a');link.href='#'+id;link.dataset.example=id;const icon=document.createElement('span');icon.className='nav-icon';icon.textContent=example.icon;const label=document.createElement('span');label.textContent=example.title;link.append(icon,label);$('navigation').append(link);}
function navigate(){
  scope.Dispose();scope=new CompositeDisposable();$('events').replaceChildren();eventCount=0;$('event-count').textContent='0';
  const key=location.hash.slice(1)||'overview';const example=examples[key]??examples.overview;
  document.querySelectorAll<HTMLAnchorElement>('[data-example]').forEach(link=>{const active=link.dataset.example===(examples[key]?key:'overview');link.classList.toggle('active',active);if(active)link.setAttribute('aria-current','page');else link.removeAttribute('aria-current');});
  $('breadcrumb').textContent=example.title;$('page-title').textContent=example.heading;$('page-description').textContent=example.description;$('lab-title').textContent=example.lab;$('lab-meta').textContent=example.meta;
  try{example.render();}catch(error){log('Example error',String(error));throw error;}
}
window.addEventListener('hashchange',navigate);
RxApp.DefaultExceptionHandler=(error:unknown)=>log('Unhandled reactive error',String(error));
navigate();
