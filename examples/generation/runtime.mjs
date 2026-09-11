import { defineViewModel, reactiveProperty } from '@wieslawsoltes/reactiveweb';
import { firstValueFrom, map } from 'rxjs';

const Counter = defineViewModel({
  name: 'Counter',
  properties: {
    Count: reactiveProperty(1, { validate: count => Number.isInteger(count) && count >= 0 }),
    Tags: reactiveProperty([]),
  },
  computed: {
    Double: { source: vm => vm.WhenAnyValue('Count').pipe(map(count => count * 2)), initialValue: 0 },
  },
  commands: {
    Add: { execute: (vm, amount) => vm.Count += amount },
  },
});

const viewModel = new Counter({ Count: 3 });
await firstValueFrom(viewModel.Add.Execute(2));
console.log({ Count: viewModel.Count, Double: viewModel.Double }); // { Count: 5, Double: 10 }
viewModel.Dispose();
