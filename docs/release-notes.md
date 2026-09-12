ReactiveWeb 0.1.1 updates the development toolchain and completes automated public npm distribution.

- Upgrades all seven dependency PRs: GitHub Actions, TypeScript 7, React 19.2,
  Playwright 1.63, esbuild 0.28 and type declarations.
- Fixes TypeScript 7 Node declaration discovery and isolated fixture compilation.
- Publishes the verified release tarball to npmjs with provenance after successful
  Node 22/24 validation and GitHub release creation.
- Checks release checksums, public npm integrity and distribution tags, and runs
  installed ESM, CommonJS, strict TypeScript and generator CLI consumers against
  the tarball downloaded from npm.
- Preserves Pages artifacts and the interactive showcase.

Install from npm:

```sh
npm install @wieslawsoltes/reactiveweb@0.1.1 rxjs
```

The public API is unchanged. RxJS remains an external peer dependency in npm
packages. Read docs/compatibility.md for the ReactiveUI mapping and web adaptations.

Showcase: https://wieslawsoltes.github.io/ReactiveWeb/

Assets contain the npm package, standalone browser modules, static showcase and
SHA-256 checksums. GitHub also provides full source archives.
