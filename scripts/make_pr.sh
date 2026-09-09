#!/usr/bin/env bash
# Commit everything on the current branch, push it, and open a pull request.
#
# This is the "I have never used GitHub" version of `gh pr create`: it checks the
# things a human reviewer would reject you for (secrets, huge files, no tests run)
# BEFORE it touches the network, and it prints what it is doing at every step.
#
#   ./scripts/make_pr.sh                      # uses the last commit msg or prompts
#   TITLE="my change" ./scripts/make_pr.sh    # explicit PR title
set -uo pipefail
cd "$(dirname "$0")/.."

BRANCH=$(git rev-parse --abbrev-ref HEAD)
TITLE="${TITLE:-}"
BODY="${BODY:-}"

fail() { echo "!!! $*" >&2; exit 1; }
step() { echo; echo ">>> $*"; }

[ "$BRANCH" != "main" ] || fail "you are on main; create a branch first:  git switch -c my-change"

step "quality gates (make lint + make test)"
make lint || fail "lint failed - fix it before opening a PR"
make test || fail "tests failed - fix them before opening a PR"

step "checking for secrets and oversized files"
if git status --porcelain | awk '{print $2}' | grep -Eq '(^|/)\.env$|\.pem$|credentials|\.env\.'; then
  fail "you are about to commit a credential-ish file. Add it to .gitignore instead."
fi
BIG=$(git status --porcelain | awk '{print $NF}' | while read -r f; do
        [ -f "$f" ] && [ "$(stat -c%s "$f" 2>/dev/null || stat -f%z "$f")" -gt 900000 ] && echo "$f"; done)
[ -z "$BIG" ] || fail "these files are >900KB and should not be in git: $BIG"

step "staging and committing"
git add -A
git status --short
if git diff --cached --quiet; then echo "(nothing to commit)"; else
  git commit -m "${TITLE:-wip: haweye updates}" || fail "commit failed"
fi

step "pushing origin $BRANCH"
git push -u origin "$BRANCH" || fail "push rejected - see docs/09-github-for-beginners.md"

step "opening the pull request"
if ! command -v gh >/dev/null 2>&1; then
  echo "gh is not installed; open the PR in the browser instead:"
  echo "  https://github.com/$(git config --get remote.origin.url | sed -E 's#.*github.com[:/]##; s#\.git$##')/pull/new/$BRANCH"
  exit 0
fi
[ -n "$TITLE" ] || TITLE="$(git log -1 --pretty=%s)"
[ -n "$BODY" ] || BODY=$'## What changed\n\n(fill in a sentence or two)\n\n## How to test\n\n```bash\nmake lint && make test\n```\n\n- [ ] I ran the quality gates locally\n- [ ] I updated docs when behaviour changed'
gh pr create --fill --title "$TITLE" --body "$BODY" --base main --head "$BRANCH" || \
  gh pr create --title "$TITLE" --body "$BODY" --base main --head "$BRANCH"
echo "done: $(gh pr view --json url -q .url 2>/dev/null || echo 'check the repository')"
