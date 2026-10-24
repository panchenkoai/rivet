# Homebrew tap for Rivet

```bash
brew install rivet-lab/tap/rivet
```

The tap is [`rivet-lab/homebrew-tap`](https://github.com/rivet-lab/homebrew-tap); the formula is
`Formula/rivet.rb`. Homebrew on Linux installs the `-glibc2.28` build, which runs on the Ubuntu
22.04 image Homebrew uses.

## Automatic updates after each release

The [Release workflow](../../.github/workflows/release.yml) regenerates `packaging/homebrew/rivet.rb`
from `artifacts/SHA256SUMS.txt` and pushes it to the tap. The push authenticates as the
`rivet-release-bot` GitHub App of the `rivet-lab` organisation:

- the App is installed on `homebrew-tap` only, with **Contents: Read and write**;
- the rivet repository holds its id and private key as the secrets `TAP_APP_ID` and
  `TAP_APP_PRIVATE_KEY`;
- each release mints a token that lives one hour (`actions/create-github-app-token`), so there is no
  personal token to renew.

A failed push fails the release job. To rotate the key: App settings → Private keys → Generate, set
`TAP_APP_PRIVATE_KEY` to the new `.pem`, then delete the old key there.

## Manual refresh

```bash
python3 -m dev.pytools.homebrew_formula --tag v0.32.2                 # downloads SHA256SUMS.txt
python3 -m dev.pytools.homebrew_formula --tag v0.32.2 --sums-file path/to/SHA256SUMS.txt
cp packaging/homebrew/rivet.rb ../homebrew-tap/Formula/rivet.rb
```

Then commit and push the tap, and check `brew install rivet-lab/tap/rivet && brew test rivet`.
