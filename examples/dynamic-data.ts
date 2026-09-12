import { ReactiveObject, Reactive, ObservableCollection, CompositeDisposable, AutoPersistCollection } from '../src/index.js';
import { DynamicData, BindChangeSet, ToDynamicDataChangeSet } from '../src/dynamic-data.js';

class Task extends ReactiveObject {
  constructor(readonly Id: number, title: string) { super(); this.Title = title; }
  @Reactive accessor Title = '';
  @Reactive accessor Done = false;
}

const lifetime = new CompositeDisposable();
const tasks = new DynamicData.SourceCache<Task, number>(task => task.Id);
lifetime.Add(tasks);
const binding = BindChangeSet(tasks.Connect().pipe(
  DynamicData.AutoRefresh<Task, number>(),
  DynamicData.Filter<Task, number>(task => !task.Done),
  DynamicData.Sort<Task, number>((left, right) => left.Title.localeCompare(right.Title)),
));
lifetime.Add(binding);
lifetime.Add(binding.Collection.ItemsChanged.subscribe(items => console.log(items.map(task => task.Title))));
const task = new Task(1, 'Publish the library');
lifetime.Add(task);
tasks.AddOrUpdate(task);
task.Done = true;

const legacy = new ObservableCollection<Task>();
lifetime.Add(legacy);
lifetime.Add(BindChangeSet(ToDynamicDataChangeSet(legacy).pipe(DynamicData.Filter(task => !task.Done))));
legacy.Add(task);
lifetime.Add(AutoPersistCollection(tasks, item => Promise.resolve({ Id: item.Id, Done: item.Done }), { throttleMs: 300 }));
lifetime.Dispose();
