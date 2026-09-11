import { ReactiveElement } from '@wieslawsoltes/reactiveweb/html';
import { CounterViewModel } from './counter.generated.js';

/** HTML equivalent of a typed IViewFor<TViewModel> host. */
export class GeneratedCounterView extends ReactiveElement<CounterViewModel> {
  readonly #ownedViewModel = new CounterViewModel();

  constructor() {
    super();
    this.ViewModel = this.#ownedViewModel;
    this.attachShadow({ mode: 'open' }).innerHTML = `
      <style>:host { display:block; font:16px system-ui; padding:1rem; } button { font:inherit; }</style>
      <h2 data-rx-text="Name"></h2>
      <label>Name <input data-rx-bind="Name"></label>
      <p>Count: <strong data-rx-text="Count"></strong></p>
      <p>Double: <output data-rx-text="Double"></output></p>
      <button data-rx-command="Increment">Increment</button>
    `;
  }

  override Dispose(): void {
    super.Dispose();
    this.#ownedViewModel.Dispose();
  }
}

if (!customElements.get('generated-counter')) customElements.define('generated-counter', GeneratedCounterView);
