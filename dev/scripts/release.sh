#!/usr/bin/env bash
# Tag a Rivet release and push the tag (triggers .github/workflows/release.yml).
#
# Prerequisites:
#   - Cargo.toml version matches the tag you want (e.g. 0.7.5 → v0.7.5)
#   - CHANGELOG.md has a section starting with "## <version> "
#   - Release checklist green: docs/release-checklist.md
#
# Usage:
#   ./dev/scripts/release.sh --dry-run          # show what would happen
#   ./dev/scripts/release.sh                    # create annotated tag locally
#   ./dev/scripts/release.sh --push             # tag + git push origin vX.Y.Z
#   ./dev/scripts/release.sh --message "summary" --push
#
# After the workflow finishes:
#   - GitHub Release: binaries, SHA256SUMS.txt, notes from CHANGELOG
#   - GHCR image: ghcr.io/<owner>/rivet:vX.Y.Z and :latest
#   - Homebrew tap: auto-updated if HOMEBREW_TAP_GITHUB_TOKEN is set

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CARGO_TOML="${ROOT}/Cargo.toml"
CHANGELOG="${ROOT}/CHANGELOG.md"

DRY_RUN=0
PUSH=0
TAG_OVERRIDE=""
MESSAGE=""

usage() {
  cat <<'EOF'
Usage: dev/scripts/release.sh [--dry-run] [--push] [--tag vX.Y.Z] [--message TEXT]

Options:
  --dry-run     Validate only; do not create or push a tag
  --push        Push the tag to origin after creating it
  --tag vX.Y.Z  Override tag (default: v<version from Cargo.toml>)
  --message     Annotated tag message (default: vX.Y.Z: <CHANGELOG subtitle>)
  -h, --help    Show this help
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --push) PUSH=1; shift ;;
    --tag)
      TAG_OVERRIDE="${2:-}"
      [[ -n "$TAG_OVERRIDE" ]] || usage
      shift 2
      ;;
    --message)
      MESSAGE="${2:-}"
      [[ -n "$MESSAGE" ]] || usage
      shift 2
      ;;
    -h|--help) usage ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
done

VERSION="$(grep -E '^version[[:space:]]*=' "$CARGO_TOML" | head -1 | sed -n 's/^version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p')"
if [[ -z "$VERSION" ]]; then
  echo "error: could not parse version from Cargo.toml" >&2
  exit 1
fi

if [[ -n "$TAG_OVERRIDE" ]]; then
  TAG="$TAG_OVERRIDE"
  if [[ "$TAG" == v* ]]; then
    TAG_VERSION="${TAG#v}"
  else
    echo "error: --tag must look like vX.Y.Z (got: $TAG)" >&2
    exit 1
  fi
  if [[ "$TAG_VERSION" != "$VERSION" ]]; then
    echo "error: Cargo.toml version is ${VERSION}, but --tag is ${TAG}" >&2
    exit 1
  fi
else
  TAG="v${VERSION}"
fi

if [[ ! -f "$CHANGELOG" ]]; then
  echo "error: CHANGELOG.md not found" >&2
  exit 1
fi

if ! awk -v ver="$VERSION" '$0 ~ "^## " ver " " { found = 1; exit } END { exit !found }' "$CHANGELOG"; then
  echo "error: CHANGELOG.md has no section starting with '## ${VERSION} '" >&2
  exit 1
fi

if git -C "$ROOT" rev-parse "$TAG" >/dev/null 2>&1; then
  echo "error: tag ${TAG} already exists locally" >&2
  exit 1
fi

if ! git -C "$ROOT" diff --quiet || ! git -C "$ROOT" diff --cached --quiet; then
  echo "error: working tree is not clean — commit or stash before tagging" >&2
  exit 1
fi

HEAD="$(git -C "$ROOT" rev-parse --short HEAD)"
BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)"

if [[ -z "$MESSAGE" ]]; then
  SUBTITLE="$(awk -v ver="$VERSION" '
    $0 ~ "^## " ver " " {
      sub(/^## [0-9.]+ \(unreleased\) — /, "")
      sub(/^## [0-9.]+ — /, "")
      sub(/^## [0-9.]+ /, "")
      print
      exit
    }
  ' "$CHANGELOG")"
  if [[ -n "$SUBTITLE" ]]; then
    MESSAGE="${TAG}: ${SUBTITLE}"
  else
    MESSAGE="${TAG}"
  fi
fi

cat <<EOF
Rivet release
  tag:     ${TAG}
  version: ${VERSION}
  commit:  ${HEAD} (${BRANCH})
  message: ${MESSAGE}
  push:    $([[ "$PUSH" -eq 1 ]] && echo yes || echo no)
  CI:      .github/workflows/release.yml (on tag push)
EOF

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo
  echo "Dry run — no tag created."
  exit 0
fi

git -C "$ROOT" tag -a "$TAG" -m "$MESSAGE"
echo "Created tag ${TAG}"

if [[ "$PUSH" -eq 1 ]]; then
  git -C "$ROOT" push origin "$TAG"
  echo "Pushed ${TAG} — watch: https://github.com/panchenkoai/rivet/actions/workflows/release.yml"
else
  echo
  echo "Next: git push origin ${TAG}"
fi
