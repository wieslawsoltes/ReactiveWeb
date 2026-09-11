import { ReactiveObject, Reactive, ToProperty, ReactiveCommand } from '@wieslawsoltes/reactiveweb';
import { map } from 'rxjs';

/** TypeScript 5+ standard decorators: do not enable experimentalDecorators. */
export class PersonViewModel extends ReactiveObject {
  @Reactive<string>({ dependents: ['DisplayName'] }) accessor FirstName = 'Ada';
  @Reactive<string>({
    validate: value => value.trim().length > 0 || 'A last name is required.',
    dependents: ['DisplayName'],
  }) accessor LastName = 'Lovelace';

  get DisplayName(): string { return `${this.FirstName} ${this.LastName}`; }
  declare readonly NameLength: number;
  readonly #nameLength = ToProperty(
    this.WhenAnyValue<string>('LastName').pipe(map(name => name.length)),
    this, 'NameLength', { initialValue: 0 },
  );
  readonly Reset = ReactiveCommand.Create<void, void>(() => { this.LastName = 'Lovelace'; });

  override Dispose(): void {
    this.Reset.Dispose();
    this.#nameLength.Dispose();
    super.Dispose();
  }
}
