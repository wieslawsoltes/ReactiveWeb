# Distribution and publishing

## Automated main branch workflow

`.github/workflows/ci.yml` runs a Node 22/24 verification matrix on pull requests and
main commits. Both versions build, strictly typecheck examples, run behavioral
tests, install the real tarball in an isolated consumer, and exercise the showcase
with Chromium. Failure prevents distribution jobs.

A successful main build produces:

* An npm-compatible `wieslawsoltes-reactiveweb-VERSION.tgz`.
* `reactiveweb-browser.tar.gz`, including shared browser chunks and license files.
* `reactiveweb-showcase.tar.gz`, the static sample application.
* `SHA256SUMS.txt` for downloaded artifact verification.
* A GitHub Pages artifact deployed through the `github-pages` environment.
* A GitHub Packages version and a GitHub release tagged `vVERSION` if new.

The release and Pages jobs are independent after package validation. Package
publication uses `GITHUB_TOKEN` with scoped `packages: write`; creating the release
uses `contents: write`. Pages deployment uses `pages: write` and OIDC. The workflow
does not request repository administration or store a personal GitHub token.

## Repository setup

GitHub Pages must have **Settings → Pages → Build and deployment → Source: GitHub
Actions** selected. The repository must allow Actions, package publication and the
`github-pages` deployment environment. GitHub may apply environment protections
configured by the owner. These are external settings; workflow files do not change
administration policy.

GitHub Packages generally requires authentication even to install public packages.
For consumers, configure `@wieslawsoltes:registry=https://npm.pkg.github.com` and use
an appropriate read credential. Installing the public release `.tgz` avoids that
registry requirement. See [GitHub's npm registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry).

## Release a new version

1. Update `package.json` and lockfile with `npm version patch --no-git-tag-version`
   (or minor/major as appropriate).
2. Update `CHANGELOG.md`, `docs/release-notes.md`, and visible showcase version.
3. Run `npm run check`, `npm run test:package`, and `npm run test:browser`.
4. Commit and merge/push to main. CI creates `vVERSION`, publishes the package,
   attaches the verified assets and deploys the showcase.

Existing package versions and release assets are immutable. A later main commit
with the same version deploys the showcase but retains the existing release and
package. Bump the package version for changes intended for distribution.

## Optional public npmjs publishing

`.github/workflows/npm-publish.yml` accepts an existing release tag and distribution
tag (`latest` or `next`). It checks out that version, validates the tag against the
package, rebuilds/tests, checks the packaged consumers and publishes with
provenance. This workflow does not silently claim npmjs publication on main.

The npm package owner must configure a trusted publisher on npmjs for:

| Field | Value |
| --- | --- |
| Organization/user | `wieslawsoltes` |
| Repository | `ReactiveWeb` |
| Workflow filename | `npm-publish.yml` |
| Environment | `npm` |

The workflow uses Node 24 and npm 11 to meet npm's OIDC requirements. An `NPM_TOKEN`
environment/repository secret can be used for a first publish or fallback if the
owner chooses token-based authentication. Never place a token in tracked `.npmrc`
or source. Package ownership and first-publication requirements must be satisfied
on npmjs; they are outside the source repository. See [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/).

## Reproduce artifacts locally

```sh
npm ci
npm run check
npm run test:package
npm run test:browser
npm pack --ignore-scripts
tar -czf reactiveweb-browser.tar.gz -C dist .
tar -czf reactiveweb-showcase.tar.gz -C site .
sha256sum *.tgz *.tar.gz > SHA256SUMS.txt
```

The library is independent of the sample's CSS, navigation, React rendering and
hosting. Consumers can choose only the core or specific adapter entry points.
