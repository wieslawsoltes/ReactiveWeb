import { Observable, distinctUntilChanged, map } from 'rxjs';
import { Disposable, dispose, type IDisposable, type DisposableLike } from './disposables.js';
import { ObservableCollection } from './collections.js';
import { ReactiveCommand } from './command.js';

export interface IScreen { readonly Router: RoutingState; }
export interface IRoutableViewModel { readonly UrlPathSegment: string; readonly HostScreen: IScreen; }

export class RoutingState<T extends IRoutableViewModel = IRoutableViewModel> implements IDisposable {
  readonly NavigationStack = new ObservableCollection<T>();
  readonly CurrentViewModel: Observable<T | null>;
  readonly Navigate: ReactiveCommand<T, T>;
  readonly NavigateBack: ReactiveCommand<void, T | null>;
  readonly NavigateAndReset: ReactiveCommand<T, T>;
  constructor() {
    this.CurrentViewModel = this.NavigationStack.ItemsChanged.pipe(map(stack => stack.at(-1) ?? null), distinctUntilChanged());
    this.Navigate = ReactiveCommand.Create<T, T>(viewModel => {
      this.validate(viewModel); this.NavigationStack.Add(viewModel); return viewModel;
    });
    this.NavigateBack = ReactiveCommand.Create<void, T | null>(() => {
      if (this.NavigationStack.Count > 1) this.NavigationStack.RemoveAt(this.NavigationStack.Count - 1);
      return this.CurrentViewModelValue;
    }, this.NavigationStack.CountChanged.pipe(map(count => count > 1)));
    this.NavigateAndReset = ReactiveCommand.Create<T, T>(viewModel => {
      this.validate(viewModel); this.NavigationStack.Reset([viewModel]); return viewModel;
    });
  }
  private validate(viewModel: T): void {
    if (!viewModel || typeof viewModel.UrlPathSegment !== 'string') throw new TypeError('Navigation requires an IRoutableViewModel with UrlPathSegment');
  }
  get CurrentViewModelValue(): T | null { return this.NavigationStack.Items.at(-1) ?? null; }
  get CurrentPath(): string { return this.NavigationStack.Items.map(item => item.UrlPathSegment).join('/'); }
  FindViewModelInStack<TView extends T>(type: new (...args: any[]) => TView): TView | undefined {
    return [...this.NavigationStack.Items].reverse().find(item => item instanceof type) as TView | undefined;
  }
  Dispose(): void { this.Navigate.Dispose(); this.NavigateBack.Dispose(); this.NavigateAndReset.Dispose(); this.NavigationStack.Dispose(); }
  unsubscribe(): void { this.Dispose(); }
}

export function WhenNavigatedTo(viewModel: IRoutableViewModel): Observable<void>;
export function WhenNavigatedTo(viewModel: IRoutableViewModel, onNavigate: () => DisposableLike): IDisposable;
export function WhenNavigatedTo(viewModel: IRoutableViewModel, onNavigate?: () => DisposableLike): Observable<void> | IDisposable {
  if (onNavigate) {
    let resource: DisposableLike, current = false;
    const subscription = viewModel.HostScreen.Router.CurrentViewModel.subscribe(value => {
      const isCurrent = value === viewModel;
      if (isCurrent && !current) { current = true; resource = onNavigate(); }
      else if (!isCurrent && current) { current = false; const old = resource; resource = undefined; dispose(old); }
    });
    return Disposable.Create(() => { subscription.unsubscribe(); dispose(resource); resource = undefined; });
  }
  return new Observable(subscriber => {
    let wasCurrent = false;
    return viewModel.HostScreen.Router.CurrentViewModel.subscribe(current => {
      const isCurrent = current === viewModel;
      if (isCurrent && !wasCurrent) subscriber.next();
      wasCurrent = isCurrent;
    });
  });
}
export function WhenNavigatedFrom(viewModel: IRoutableViewModel): Observable<void> {
  return new Observable(subscriber => {
    let wasCurrent = false;
    return viewModel.HostScreen.Router.CurrentViewModel.subscribe(current => {
      const isCurrent = current === viewModel;
      if (!isCurrent && wasCurrent) subscriber.next();
      wasCurrent = isCurrent;
    });
  });
}
export function IsCurrentViewModel(viewModel: IRoutableViewModel): Observable<boolean> {
  return viewModel.HostScreen.Router.CurrentViewModel.pipe(map(current => current === viewModel), distinctUntilChanged());
}

export const WhenNavigatedToObservable = (viewModel: IRoutableViewModel): Observable<void> => WhenNavigatedTo(viewModel);
export const WhenNavigatingFromObservable = WhenNavigatedFrom;
