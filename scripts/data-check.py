#!/usr/bin/env python3
"""Check that git tracks nothing under data/ except its README.md and .gitignore.

data/ holds real inputs (.agents/rules/100-overview.md, prime directive 1).
data/.gitignore keeps them out of git; this check catches what it cannot:
a file added with `git add -f`, or a re-include pattern that matches more than it should.
It reads the index, so a staged file fails the check before it is committed.
Standard library only; needs git.
"""

import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ALLOWED = {"data/.gitignore", "data/README.md"}


def main():
    try:
        out = subprocess.run(["git", "-C", ROOT, "ls-files", "-z", "--", "data"],
                             capture_output=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as e:
        print(f"data-check: cannot list tracked files: {e}", file=sys.stderr)
        return 1

    # Paths are bytes: a name that is not UTF-8 still fails the check, shown with its bytes escaped.
    paths = (p.decode("utf-8", "backslashreplace") for p in out.split(b"\0") if p)
    extra = sorted(p for p in paths if p not in ALLOWED)
    for p in extra:
        print(f"{p}: tracked or staged under data/; unstage it with 'git rm --cached'", file=sys.stderr)
    if extra:
        print(f"data-check: {len(extra)} file(s) under data/ must stay local", file=sys.stderr)
        return 1

    print("data-check: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
