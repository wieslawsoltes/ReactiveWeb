# Observable scheduling, switching, and diagnostics

ReactiveWeb uses RxJS observables and schedulers throughout. These helpers preserve familiar .NET names while accepting normal RxJS `Observable`, `Observer`, `SchedulerLike`, and subscription objects. They do not modify the RxJS prototype. Import RxJS operators directly from `rxjs` when no ReactiveWeb adaptation is needed.

## ScheduledSubject

```ts
import { asyncScheduler } from 'rxjs';
import { ScheduledSubject } from '@wieslawsoltes/reactiveweb';

const notifications = new ScheduledSubject<string>(
  asyncScheduler,
  { next: value => console.info('No view subscribed:', value) },
);

const subscription = notifications.Subscribe(value => console.info('View:', value));
notifications.OnNext('Saved');
subscription.Dispose();
notifications.Dispose();
```

The constructor accepts `(scheduler?, defaultObserver?, defaultSubject?)`. The default scheduler is `RxApp.MainThreadScheduler`; the optional backing subject can be a `Subject`, `BehaviorSubject`, or `ReplaySubject`. Every subscriber's notifications, including replayed values, errors, and completion, are scheduled independently. Disposing a subscription cancels its pending notifications.

The default observer is connected only when there are no real subscribers. It is disconnected when the first real subscriber arrives, including cancellation of already scheduled default deliveries, and restored after the last subscriber leaves. `HasObservers` includes this fallback subscription; `ObserverCount` counts only real subscribers. If the default observer lacks an error callback, errors route to `RxApp.HandleException`.

`OnNext`/`OnError`/`OnCompleted` have normal `next`/`error`/`complete` aliases. `Subscribe` returns a subscription supporting both `Dispose` and `unsubscribe`; inherited RxJS `subscribe` and `pipe` also work. `Dispose` cancels all queued notifications and disposes the backing subject. It does not send completion. Publishing after disposal throws. A completed or errored subject retains normal RxJS terminal behavior for later subscribers.

## WaitForDispatcherScheduler

```ts
import { animationFrameScheduler } from 'rxjs';
import { WaitForDispatcherScheduler } from '@wieslawsoltes/reactiveweb';

let viewReady = false;
const scheduler = new WaitForDispatcherScheduler(
  () => viewReady ? animationFrameScheduler : undefined,
);

// Before availability this runs on queueScheduler; later requests try again.
scheduler.schedule(() => console.info('Ready to process UI work'));
viewReady = true;
```

The default behavior matches the upstream fallback pattern: construction, `now()`, and scheduling try the provider until it succeeds, then cache the returned scheduler. Unavailable work uses `queueScheduler` immediately. There is no background polling in this mode. A provider can return `undefined`/`null` or throw while unavailable; `LastProviderError` retains a thrown error for inspection and is cleared on success. Unlike the CLR implementation's named exception filter, the web provider can report unavailability with any thrown JavaScript value.

Explicit waiting is available for an application that must delay work until its view exists:

```ts
const scheduler = new WaitForDispatcherScheduler(provider, {
  waitForDispatcher: true,
  retryDelay: 16,
  // retryScheduler defaults to asyncScheduler; a TestScheduler is useful in tests.
});
const work = scheduler.schedule(() => renderView(), 100);
// work.unsubscribe() cancels both pending retries and pending execution.
```

Wait mode retries at a strictly positive interval. It performs no synchronous polling or busy waiting. A requested delay is measured from the original scheduling request; once the dispatcher is ready, only the remaining delay is applied. Work waits indefinitely until availability, cancellation, or scheduler disposal. Recursive RxJS actions using `this.schedule(...)` are supported. `Dispose` cancels scheduler-owned pending work; further scheduling throws. `IsReady`, `IsDisposed`, `Now`, and a `.NET`-style `Schedule(state, action, delay?)` are available alongside `SchedulerLike` methods.

## Following replaceable observables and commands

```ts
import { SwitchSelect, SwitchSubscribe } from '@wieslawsoltes/reactiveweb';

const busy$ = commandProperty$.pipe(SwitchSelect(command => command.IsExecuting));
const subscription = SwitchSubscribe(commandProperty$, command => command.Results, {
  next: value => showResult(value),
  error: error => showError(error),
});

// A stream that already emits observables needs no selector.
const results = SwitchSubscribe(streamOfStreams$, value => showResult(value));
```

`SwitchSelect(selector)` is a normal RxJS operator. `SwitchSelect(source, selector)` is its direct-function equivalent. `SwitchSubscribe(source, observerOrOnNext)` subscribes eagerly to a stream of observables; `SwitchSubscribe(source, selector, observerOrOnNext)` projects objects to streams first. Both return disposable subscriptions. For error and completion handlers, use an observer object; this avoids the ambiguity between JavaScript callback and selector overloads.

A new non-null outer value unsubscribes the previous inner stream. Null and undefined outer values are ignored and leave the current inner attached, matching the inspected upstream implementation. Completion waits for both the outer source and its active inner; any error terminates the result. Missing `SwitchSubscribe` error callbacks route errors through `RxApp`. For stream transformation without subscribing, use `SwitchSelect`.

## Side effects, logs, and recovery

| Helper | Operator form | Direct form | Behavior |
| --- | --- | --- | --- |
| `Do` | `Do(observerOrNext, error?, complete?)` | `Do(source, observerOrNext, error?, complete?)` | RxJS `tap` semantics; side effects precede unchanged notifications. |
| `Log` | `Log(logger?, message?, stringifier?)` | `Log(source, logger?, message?, stringifier?)` | Logs each `OnNext`, `OnError`, and `OnCompleted`; passes the source through. |
| `LoggedCatch` | `LoggedCatch(logger, fallback?, message?, shouldCatch?)` | `LoggedCatch(source, logger, fallback?, message?, shouldCatch?)` | Logs a matching source error, then switches once to fallback. |

Loggers can be browser-console-shaped objects with `info`/`warn`, .NET-style objects with `Info`/`Warn`, or objects exposing `Log()` to return either logger shape. The default logger is `console`. A stringifier changes only the value sent to the logger. Exceptions thrown by a logger, side-effect callback, selector, predicate, or fallback factory enter the observable error channel.

```ts
import { EMPTY, of } from 'rxjs';
import { Do, Log, LoggedCatch } from '@wieslawsoltes/reactiveweb';

const recovered$ = request$.pipe(
  Do(value => updateStatus(value)),
  Log(console, 'Load'),
  LoggedCatch(console, error => of(cachedValue), 'Using cached value'),
);

// Explicitly swallow a failure without emitting a replacement value.
const ignored$ = request$.pipe(LoggedCatch(console, EMPTY));
```

An omitted `LoggedCatch` fallback emits **one `undefined` value** and completes, corresponding to the upstream implementation's `default(T)` emission. Its TypeScript return type includes `undefined`. Supply `EMPTY` for zero values or a typed observable/factory for an explicit replacement. `shouldCatch(error)` replaces CLR generic exception-type filtering; unmatched errors propagate without logging. Errors from the fallback propagate and are not caught repeatedly.

Camel-case aliases are `switchSelect`, `switchSubscribe`, `loggedCatch`, and `logObservable`. Grouped exports `ObservableLoggingMixins` and `SwitchSubscribeMixins` support code organized around familiar .NET extension-class names.

The inspected upstream implementations are `ReactiveUI.Shared/Scheduler/ScheduledSubject.cs`, `WaitForDispatcherScheduler.cs`, `Mixins/SwitchSubscribeMixins.cs`, and `ObservableLoggingMixins.cs` at ReactiveUI commit `d2c79cf324b1d3f6172b0fcc3bd54bbf18f27151`. Web waiting mode, JavaScript callback shapes, exception predicates, and numeric millisecond scheduler times are explicit adaptations.
