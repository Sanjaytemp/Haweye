#!/usr/bin/env python3
"""Download Maven artifacts into a target directory (no curl/wget dependency).

Usage:  download_jars.py <target_dir> <group:artifact:version> [...]
If <target_dir>/<artifact>-<version>.jar already exists it is skipped, so
re-building the docker image is cheap while the cache is kept warm.
"""
from __future__ import annotations

import os
import sys
import urllib.request


def coord_to_url(coord: str) -> tuple[str, str]:
    group, artifact, version = coord.split(":")
    name = f"{artifact}-{version}.jar"
    path = f"{group.replace('.', '/')}/{artifact}/{version}/{name}"
    base = os.environ.get("MAVEN_BASE_URL", "https://repo1.maven.org/maven2")
    return f"{base}/{path}", name


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    target = sys.argv[1]
    os.makedirs(target, exist_ok=True)
    for coord in sys.argv[2:]:
        url, name = coord_to_url(coord)
        dest = os.path.join(target, name)
        if os.path.exists(dest) and os.path.getsize(dest) > 10_000:
            print(f"[skip] {name} already present")
            continue
        print(f"[get ] {url}")
        tmp = dest + ".part"
        with urllib.request.urlopen(url, timeout=180) as resp, open(tmp, "wb") as fh:
            total = int(resp.headers.get("Content-Length") or 0)
            done = 0
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                fh.write(chunk)
                done += len(chunk)
                if total:
                    print(f"\r       {done * 100 // total:3d}% of {total // 1024} KiB", end="")
        print()
        os.replace(tmp, dest)
        print(f"[ok  ] {dest} ({os.path.getsize(dest) // 1024} KiB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
