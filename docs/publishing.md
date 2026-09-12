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
* A public npmjs version, followed by verification of its downloaded package,
  when the release tag identifies the verified main commit.

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
   attaches the verified assets, publishes that exact release tarball to npmjs,
   verifies its public installation, and deploys the showcase.

Existing package versions and release assets are immutable. A later main commit
with the same version deploys the showcase but retains the existing release and
package. Bump the package version for changes intended for distribution.

## Public npmjs publishing

`.github/workflows/npm-publish.yml` is called automatically after the main CI
release job. It also accepts manual dispatch with an existing release `tag`, an
optional full `expected_sha`, and a distribution `dist_tag` (`latest` or `next`).
The automatic caller supplies the exact verified commit. It skips publication on
later main commits that retain a version whose release belongs to an earlier
commit. Failed publication can be retried by rerunning the failed CI job, or by
manually dispatching the publication workflow for the release tag.
This asset-verification workflow supports release tags from `v0.1.1` onward;
older tags do not contain the registry verification script or tarball test option.

The workflow checks out `refs/tags/TAG`, validates the package version and commit,
and downloads the existing release tarball and `SHA256SUMS.txt`. It verifies that
checksum and runs the isolated ESM, CommonJS, TypeScript, RxJS identity, and
generator CLI consumers against the downloaded tarball. Publication uses that
same archive with `--ignore-scripts`, so no build runs between verification and
publication.

Set the repository or `npm` environment secret `NPM_TOKEN` to a token authorized
to publish `@wieslawsoltes/reactiveweb`, or configure npm trusted publishing. The
token is available only in the publication step, not during dependency
installation, package tests, or public registry verification. Package ownership,
token permissions, and any account two-factor requirements must permit
noninteractive publication; the workflow does not change those account settings.

For trusted publishing, use these npmjs settings:

| Field | Value |
| --- | --- |
| Organization/user | `wieslawsoltes` |
| Repository | `ReactiveWeb` |
| Workflow filename, automatic publication | `ci.yml` |
| Workflow filename, manual publication | `npm-publish.yml` |
| Environment | `npm` |

For reusable workflows, npm checks the calling workflow's identity. Configure
both identities if both entry points should use OIDC, and allow direct
`npm publish` when configuring the publisher. The caller and called job both
grant `id-token: write`. Node 24 and npm 11 support OIDC authentication with token
fallback and provenance generation. Never put a token in tracked `.npmrc` or
source. See [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/).

`scripts/npm-registry.mjs` queries the public registry without credentials. A
confirmed 404 permits publication. An existing version is skipped only when its
SHA512 integrity equals the release bytes; a mismatch or permanent registry error
fails. After publication it waits up to five minutes for the version, requested
distribution tag, installable package index, and provenance metadata to appear, downloads the public npm
tarball, compares its SHA512, reruns the installed consumers, and checks a fresh
anonymous installation by package name. This checks the
presence of provenance metadata, not a separate cryptographic attestation audit.
An existing version whose distribution tag has moved to another version is not
silently retagged; verification reports that mismatch.

The reusable workflow interface is:

```yaml
npm:
  needs: release
  if: needs.release.outputs.publish == 'true'
  permissions:
    contents: read
    id-token: write
  uses: ./.github/workflows/npm-publish.yml
  with:
    tag: ${{ needs.release.outputs.tag }}
    expected_sha: ${{ github.sha }}
    dist_tag: latest
  secrets:
    NPM_TOKEN: ${{ secrets.NPM_TOKEN }}
```

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
