#!/usr/bin/env python3
"""Check that relative links and `docs/NN-*.md` citations resolve.

Beginner-facing repos rot in a specific way: a doc gets renamed and thirty
cross-references become 404s. This is 60 lines of python instead of a
markdown-link-checker dependency.

    python3 scripts/check_docs_links.py [--root .]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

LINK = re.compile(r"\[[^\]]*\]\((?!https?://|mailto:)([^)#\s]+)(#[^)]*)?\)")
CITE = re.compile(r"docs/([0-9A-Za-z._-]+\.md)")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="validate relative links in markdown")
    ap.add_argument("--root", default=".")
    args = ap.parse_args(argv)
    root = Path(args.root).resolve()
    broken: list[str] = []

    for md in sorted(root.rglob("*.md")):
        if any(part in {".venv", "node_modules", ".git"} for part in md.parts):
            continue
        for target, _anchor in LINK.findall(md.read_text()):
            if target.startswith("/"):
                broken.append(f"{md.relative_to(root)}: absolute link {target}")
                continue
            dest = (md.parent / target).resolve()
            if not dest.exists():
                broken.append(f"{md.relative_to(root)} -> {target}")

    # code comments may cite docs/NN-*.md; those files must exist
    docs = {p.name for p in (root / "docs").glob("*.md")}
    for src in list(root.rglob("*.py")) + list(root.rglob("*.sh")) + list(root.rglob("Makefile")):
        if any(part in {".venv", "node_modules", ".git"} for part in src.parts):
            continue
        try:
            text = src.read_text()
        except UnicodeDecodeError:
            continue
        for name in CITE.findall(text):
            if name not in docs:
                broken.append(f"{src.relative_to(root)} -> docs/{name}")

    if broken:
        print("broken documentation references:", file=sys.stderr)
        for line in sorted(set(broken)):
            print(f"  {line}", file=sys.stderr)
        print("\nfix: create/rename the file, or correct the link.", file=sys.stderr)
        return 1
    print(">>> all relative links and doc citations resolve")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
