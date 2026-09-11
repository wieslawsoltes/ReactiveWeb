import { map } from 'rxjs';
import type { CounterViewModel } from './counter.generated.js';

export const doubleCount = (viewModel: CounterViewModel) =>
  viewModel.WhenAnyValue<number>('Count').pipe(map(count => count * 2));
export const canIncrement = (viewModel: CounterViewModel) =>
  viewModel.WhenAnyValue<number>('Count').pipe(map(count => count < 1000));
export function increment(viewModel: CounterViewModel, _input: void): number {
  return ++viewModel.Count;
}
