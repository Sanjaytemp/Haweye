# 09 — Git and GitHub, from absolutely nothing

*You said you don't know GitHub. This file assumes you don't know `cd` either. It
is written for this repository specifically, so every command is one you actually
need here.*

## 0. The two things, once and for all

* **git** = a program on your laptop that records the history of a folder. It works
  with no internet. Your history lives in `Haweye/.git/`.
* **GitHub** = a website that stores a copy of that folder and adds review
  machinery (issues, pull requests, CI). Deleting GitHub would not lose your
  history; deleting `.git` would.

Analogy that has never failed me: git is "save game", GitHub is "the cloud save".

## 1. Install and introduce yourself

```bash
git --version                       # if missing on macOS: xcode-select --install
git config --global user.name  "Your Name"
git config --global user.email "you@example.com"     # use your GitHub email
git config --global init.defaultBranch main
git config --global core.autocrlf input              # Windows: see docs/08
```

`--global` means "for every repo on this machine". Drop it to set a value for just
this project (common for a work email on a personal laptop).

## 2. The five verbs

Inside the project folder (`cd Haweye` — `cd` = "change directory", `pwd` = "where
am I", `ls` = "what's here"):

```bash
git status                 # what changed since the last snapshot?
git add -A                 # stage everything (put it in the box for the snapshot)
git commit -m "why I did it"
git log --oneline -10      # the last 10 snapshots
git diff                   # unstaged changes, line by line
```

A commit is **local**. Nobody else can see it until:

```bash
git push                  # upload my commits to GitHub
git pull --rebase         # download GitHub's commits and replay mine on top
```

`origin` = "the GitHub copy". `main` = "the branch everyone agrees on".
`git push -u origin my-branch` sets the pairing so later you can type just
`git push`.

## 3. Branches: why and how

You never commit straight to `main` in a shared repo, because `main` should always
be "the version that works". A **branch** is a second label pointing at your last
commit; your new commits move your label, not `main`'s.

```bash
git switch -c add-cdc-check     # create + move onto a new branch
# ...make changes...
git add -A && git commit -m "cdc parity check script"
git push -u origin add-cdc-check
```

Naming: `add-x`, `fix-x`, `docs-x`, `refactor-x` — verb first, lowercase, hyphens.
That's it. You don't need `git branch -a` today; you will next week.

## 4. Your first pull request (the whole loop)

Two ways; both end the same.

**The scripted way** (this repo has it):

```bash
./scripts/make_pr.sh
```

It runs `make lint` and `make test` **before** touching the network, refuses to
commit files that look like credentials or are >900 KB, commits, pushes, and opens
the PR with `gh`. If `gh` isn't installed it prints the URL to open instead.

**The clicking way:**

1. push your branch (above);
2. GitHub shows a yellow "Compare & pull request" button on
   `https://github.com/Sanjaytemp/Haweye` — click it;
3. title: one sentence, imperative ("Add CDC parity check"); description: what and
   why, and how a reviewer can run it (`python scripts/check_cdc_parity.py`);
4. create. CI (the green/red dots) runs `make lint`, `make test`, and the pyspark
   suite;
5. review → you change things → `git add -A && git commit -m "review fixes" &&
   git push` → the PR updates by itself. **You do not open a new PR per commit.**
6. merge (squash is the kindest default for a small change).

## 5. When it says something scary

**"Your local changes would be overwritten by merge."**
git refuses to discard work silently. Either commit it, or stash it:

```bash
git stash            # put changes aside, clean tree
git pull --rebase
git stash pop        # put them back
```

**"CONFLICT (content): Merge conflict in jobs/common/rules.py"**
Both you and `main` changed the same lines. Open the file, and you'll see:

```
<<<<<<< HEAD
your lines
=======
their lines
>>>>>>> main
```

Decide what the file should look like, **delete the `<<<<<<<`, `=======`,
`>>>>>>>` lines**, then:

```bash
git add jobs/common/rules.py
git rebase --continue        # if you were rebasing
# or: git commit             # if you were merging
```

If you can't tell what "their lines" were for, read the commit that introduced
them: `git log -p -- jobs/common/rules.py | less`.

**You committed to `main` by accident.**

```bash
git switch -c rescue          # your commits are now on a branch (they follow you)
git switch main
git reset --hard origin/main  # main is clean again
git switch rescue             # keep working here
```

**You want to undo a commit that isn't pushed yet.**

```bash
git reset --soft HEAD~1       # un-commit, keep the changes staged
git reset --mixed HEAD~1      # un-commit, keep the changes unstaged
```

**You want to undo a commit you already pushed:** `git revert <sha>` — it adds an
*opposite* commit. Never `reset --hard` + force-push shared history; a teammate's
next pull becomes archaeology.

**"fatal: Need to specify how to reconcile divergent branches."**
Add once: `git config --global pull.rebase true`. Your `git pull` will now rebase
your local commits on top of the remote — the shape you almost always want.

## 6. Reading history (the superpower)

```bash
git log --oneline --graph -20
git show HEAD                     # what exactly went into the last commit
git blame jobs/common/rules.py    # who/which commit touched each line
git log -p -- jobs/common/features.py    # full diffs for one file
git diff main..my-branch          # everything your PR contains
git stash list / git fsck         # when you think you lost something: you probably didn't
```

`git log --oneline -- <file>` on a file you're about to change is the single best
habit: you see the last four reasons someone else touched it.

## 7. `.gitignore`, and the rule about secrets

Ignored = "git, don't track this". This repo already ignores `.env`, `.venv/`,
`artifacts/`, `.spark-jars/`, `.run/`, `__pycache__/`, so nothing generated by
`make` lands in a commit — except deliberately: `sql/`, `jobs/`, `docs/` are tracked.

The one rule that matters: **a secret committed once is a secret leaked once**, even
if you delete the file in the next commit — it's in history, and history is what
people clone. So:

* credentials live in `.env` (ignored). `.env.example` holds names with dummy
  values, and `make bootstrap` copies it for you;
* `scripts/make_pr.sh` refuses to commit files named like credentials, but it's a
  seatbelt, not an airbag;
* if you *did* push a token: revoke it first (assume a bot found it in minutes),
  then `git filter-repo` (or tell a maintainer) to rewrite history.

## 8. Terms you'll meet in review comments

| term | meaning |
|---|---|
| PR / MR | pull request (GitHub) / merge request (GitLab) — same thing |
| reviewer approval | a human said OK; often required by branch protection |
| CI green / red | the automated gates in `.github/workflows/ci.yml` passed/failed |
| rebase | rewrite your commits so they sit on top of current `main` |
| merge commit | keep both histories and join them (a third-parent commit) |
| squash merge | turn N commits into one on `main` (clean history, loses detail) |
| force-push | overwrite remote history. Own branches only, ever |
| detached HEAD | you checked out a commit, not a branch: `git switch -c explore` |
| fork | your own copy of someone else's repo, for when you can't push to `main` |
| tag / release | a permanent name for a commit, e.g. `v0.1.0` |

## 9. Practice loop on this repo (15 minutes, nothing can break)

```bash
git switch -c learn-git
python3 - <<'PY'
from pathlib import Path
p = Path("docs/09-github-for-beginners.md")
p.write_text(p.read_text() + "\n<!-- I edited this on purpose -->\n")
PY
git status                       # read it
git diff                         # read it
git add docs/09-github-for-beginners.md
git commit -m "docs: add a practice note"
git log --oneline -3
git push -u origin learn-git     # will fail if you have no push rights: that's fine
./scripts/make_pr.sh             # or open the PR by hand
git switch main
```

Then, on GitHub, click "Merge pull request" (squash), and locally:

```bash
git pull --rebase
git branch -d learn-git
```

You have now done the entire loop a data-engineering team uses daily: branch →
change → gates → commit → push → PR → review → merge → clean up. Everything else in
git is a footnote you look up when you need it.

## 10. If you use the web UI instead

GitHub's own editor (the pencil icon) creates a branch and a commit for you, and
offers "Open a pull request" right after. That's legitimate for docs typos. It is
*not* enough for this project, because your change won't have `make lint`/`make
test` run locally first — and CI on the PR will show you what your laptop would
have. Use the UI to learn the flow; use the terminal to work.
