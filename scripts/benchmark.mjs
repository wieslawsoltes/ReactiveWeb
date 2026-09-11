import { performance } from 'node:perf_hooks';
import { ReactiveObject, DefineReactiveProperty, WhenAnyValue, defineViewModel, ReactiveCommand, ObservableCollection } from '../dist/index.js';
const results=[];
function bench(name,iterations,action){for(let i=0;i<Math.min(iterations,10000);i++)action(i);const start=performance.now();for(let i=0;i<iterations;i++)action(i);const ms=performance.now()-start;results.push({name,iterations,milliseconds:Number(ms.toFixed(2)),operationsPerSecond:Math.round(iterations/ms*1000)});}
const vm=new ReactiveObject();DefineReactiveProperty(vm,'Count',0);let observed=0;
const subscription=WhenAnyValue(vm,'Count').subscribe(value=>{observed=value;});
bench('Reactive property write + observer',100000,i=>{vm.Count=i;});
const Generated=defineViewModel({properties:{Count:{initial:0}}});const generated=new Generated();
bench('Schema-generated property write',100000,i=>{generated.Count=i;});
const command=ReactiveCommand.Create(x=>x+1);bench('Synchronous command execution',20000,i=>command.Execute(i).subscribe());
const collection=new ObservableCollection();bench('Batched add/remove collection',10000,i=>collection.Edit(list=>{list.Add(i);list.RemoveAt(0);}));
subscription.unsubscribe();vm.Dispose();generated.Dispose();command.Dispose();collection.Dispose();
console.log(JSON.stringify({runtime:process.version,platform:process.platform,observed,results},null,2));
