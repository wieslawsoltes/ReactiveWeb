import { isObservable, type Observable } from 'rxjs';
import { Disposable, type IDisposable } from './disposables.js';

/** Providers emit property change notifications; they must not emit an initial property value. */
export interface IObservableForProperty {
  GetAffinityForObject(objectType: Function, propertyName: string, beforeChanged?: boolean): number;
  GetNotificationForProperty(sender: object, propertyName: string, beforeChanged?: boolean): Observable<unknown>;
}
/** Highest affinity wins; the most recently registered provider breaks ties. */
export class ObservablePropertyProviderRegistry {
  private readonly providers: { provider: IObservableForProperty }[] = [];
  Register(provider: IObservableForProperty): IDisposable {
    const registration = { provider }; this.providers.push(registration);
    return Disposable.Create(() => { const index = this.providers.indexOf(registration); if (index >= 0) this.providers.splice(index, 1); });
  }
  GetProvider(sender: object, propertyName: string, beforeChanged = false): IObservableForProperty | undefined {
    let best: IObservableForProperty | undefined, affinity = 0;
    for (const { provider } of this.providers) {
      const score = provider.GetAffinityForObject(sender.constructor, propertyName, beforeChanged);
      if (Number.isFinite(score) && score > 0 && score >= affinity) { affinity = score; best = provider; }
    }
    return best;
  }
  Observe(sender: object, propertyName: string, beforeChanged = false): Observable<unknown> | undefined {
    const provider = this.GetProvider(sender, propertyName, beforeChanged);
    if (!provider) return undefined;
    const stream = provider.GetNotificationForProperty(sender, propertyName, beforeChanged);
    if (!isObservable(stream)) throw new TypeError('An observable property provider must return an RxJS Observable');
    return stream;
  }
  get Count(): number { return this.providers.length; }
  Clear(): void { this.providers.length = 0; }
}
export class ObservablePropertyProviders { static Current = new ObservablePropertyProviderRegistry(); }
export function getPropertyChangeObservable(target: object, propertyName: string, beforeChange: boolean): Observable<unknown> | undefined {
  return ObservablePropertyProviders.Current.Observe(target, propertyName, beforeChange);
}
