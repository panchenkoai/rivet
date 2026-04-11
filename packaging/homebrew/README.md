# Homebrew tap for Rivet

Homebrew expects a **separate** Git repository named `homebrew-<tap>`. For `brew tap yourname/rivet`, create:

`https://github.com/yourname/homebrew-rivet`

## Layout

```
homebrew-rivet/
  Formula/
    rivet.rb
  README.md   # optional
```

## Generate `rivet.rb`

After you publish a GitHub Release (tag `v…`, assets + `SHA256SUMS.txt` from CI):

```bash
cd /path/to/rivet
chmod +x dev/update_homebrew_formula.sh

# Uses version from Cargo.toml and repository URL for GitHub owner/repo
./dev/update_homebrew_formula.sh

# Or if the app lives under another fork:
GITHUB_REPOSITORY=panchenkoai/rivet ./dev/update_homebrew_formula.sh
```

Copy the generated file:

```bash
cp packaging/homebrew/rivet.rb ../homebrew-rivet/Formula/rivet.rb
```

Commit and push the tap repo, then:

```bash
brew tap yourname/rivet
brew install rivet
brew test rivet
```

## One-time tap setup

1. Create an empty repo `yourname/homebrew-rivet` on GitHub.
2. Add `Formula/rivet.rb` (generated as above).
3. Optional: add a short README describing `brew tap yourname/rivet`.

## Automatic updates after each release

The [Release workflow](../../.github/workflows/release.yml) always regenerates `packaging/homebrew/rivet.rb` from `artifacts/SHA256SUMS.txt` (no extra download).

To **push** that formula into the tap automatically:

1. Create a GitHub repo named **`homebrew-rivet`** under the **same account/org** as Rivet  
   (e.g. `panchenkoai/homebrew-rivet` next to `panchenkoai/rivet`). Initialize with a README or leave empty; default branch `main` is fine.

2. In the **rivet** repo: **Settings → Secrets and variables → Actions → New repository secret**  
   - Name: `HOMEBREW_TAP_GITHUB_TOKEN`  
   - Value: a [fine-grained PAT](https://github.com/settings/tokens?type=beta) or classic PAT with **Contents: Read and write** on **`homebrew-rivet` only** (not the whole account if you can avoid it).

3. On every successful Release (tag `v*`), the workflow commits and pushes `Formula/rivet.rb` to `homebrew-rivet`.

If the secret is missing, the release still completes; only the tap push step is skipped.

### Manual / local refresh

Same as before — download checksums from the release or use a local `SHA256SUMS.txt`:

```bash
./dev/update_homebrew_formula.sh --tag v0.2.0-beta.2 --sums-file path/to/SHA256SUMS.txt
```
