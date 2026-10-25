#!/usr/bin/env python3
"""Check documents against .agents/rules/450-doc-lifecycle.md.

Scope: docs/ROADMAP.md, every docs/specs/<subject>/ that has a README.md, and every docs/plans/*.md.

Fails when a document lacks its line-3 Status line, uses a value outside its class or has a Status line over 160 characters,
when a subject README disagrees with the directory (rows vs files, Status, Class),
when a relative Markdown link resolves to no file,
when an active finite document is not linked from docs/ROADMAP.md or has not changed for more than 21 days,
or when a phase-table row marked done names no commit or pull request.
Standard library only; git is used for last-commit dates when available.
Set DOCS_CHECK_TODAY=YYYY-MM-DD to fix the date the staleness check measures from.
"""

import datetime
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPECS = os.path.join(ROOT, "docs", "specs")
PLANS = os.path.join(ROOT, "docs", "plans")
ROADMAP = os.path.join(ROOT, "docs", "ROADMAP.md")

STATUS_MAX = 160
STALE_DAYS = 21

VALUES = {
    "living": {"draft", "current"},
    "finite": {"draft", "active", "deferred"},
}
STATUS_RE = re.compile(r"^Status: (?P<value>[a-z-]+) \((?P<date>\d{4}-\d{2}-\d{2})\)(?: — .*)?$")
ROW_RE = re.compile(r"^\|\s*`?(?P<file>[^`|]+?)`?\s*\|\s*(?P<cls>\w+)\s*\|\s*(?P<status>[a-z-]+)\s*\|")
# A phase-table row: some cell is exactly a phase state.
PHASE_CELL_RE = re.compile(r"\|\s*(pending|in-progress|done)\s*\|")
DONE_CELL_RE = re.compile(r"\|\s*done\s*\|")
COMMIT_REF_RE = re.compile(r"\b[0-9a-f]{7,40}\b|#\d+\b")
# Inline links and images: [text](dest "title"), ![alt](dest), with <dest> allowed.
LINK_RE = re.compile(r"!?\[[^\]]*\]\(\s*(?:<(?P<angled>[^>]*)>|(?P<target>[^)\s]+))(?:\s+\"[^\"]*\")?\s*\)")
# Reference-style definitions: [id]: dest "title"
REF_RE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*(?:<(?P<angled>[^>]*)>|(?P<target>\S+))", re.M)
FENCE_RE = re.compile(r"^\s{0,3}(?P<marker>`{3,}|~{3,})")

errors = []
active = []  # (path, status date) of every active finite document


def fail(path, msg):
    errors.append(f"{os.path.relpath(path, ROOT)}: {msg}")


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def status_of(path):
    """Return the Status value and date of a document, or (None, None) after recording the error."""
    lines = read(path).split("\n")
    line3 = lines[2] if len(lines) >= 3 else ""
    m = STATUS_RE.match(line3)
    if not m:
        fail(path, "line 3 is not 'Status: <value> (<YYYY-MM-DD>) — <optional clause>'")
        return None, None
    if len(line3) > STATUS_MAX:
        fail(path, f"Status line is {len(line3)} characters; keep it to one clause within {STATUS_MAX}")

    return m.group("value"), m.group("date")


def check_document(path, cls):
    value, date = status_of(path)
    if value is not None and value not in VALUES[cls]:
        fail(path, f"Status {value!r} is not allowed for a {cls} document ({', '.join(sorted(VALUES[cls]))})")
    if cls == "finite":
        check_finite(path, value, date)


def check_finite(path, value, date):
    """Record an active document for the roadmap and staleness checks, and check its phase table."""
    if value == "active":
        active.append((path, date))
    for line in strip_fences(read(path)).split("\n"):
        if DONE_CELL_RE.search(line) and PHASE_CELL_RE.search(line) and not COMMIT_REF_RE.search(line):
            fail(path, f"phase row marked done names no commit or pull request: {line.strip()[:80]}")


def last_changed(path):
    """Return the date of the last commit touching path, today when it has uncommitted changes, or None without git."""
    rel = os.path.relpath(path, ROOT)
    try:
        dirty = subprocess.run(["git", "-C", ROOT, "status", "--porcelain", "--", rel],
                               capture_output=True, text=True, check=True).stdout.strip()
        if dirty:
            return today()
        out = subprocess.run(["git", "-C", ROOT, "log", "-1", "--format=%cs", "--", rel],
                             capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None

    return datetime.date.fromisoformat(out) if out else today()


def today():
    fixed = os.environ.get("DOCS_CHECK_TODAY")
    return datetime.date.fromisoformat(fixed) if fixed else datetime.date.today()


def roadmap_targets():
    """Return the absolute paths of the files docs/ROADMAP.md links to."""
    base = os.path.dirname(ROADMAP)
    text = strip_fences(read(ROADMAP))
    out = set()
    for m in list(LINK_RE.finditer(text)) + list(REF_RE.finditer(text)):
        target = m.group("angled") if m.group("angled") is not None else m.group("target")
        if not is_external(target):
            out.add(os.path.normpath(os.path.join(base, target.split("#", 1)[0])))

    return out


def check_active():
    linked = roadmap_targets() if os.path.isfile(ROADMAP) else set()
    for path, date in active:
        if date is None:
            continue
        if os.path.normpath(path) not in linked:
            fail(path, "active document is not linked from docs/ROADMAP.md")
        changed = datetime.date.fromisoformat(date)
        committed = last_changed(path)
        if committed is not None:
            changed = max(changed, committed)
        age = (today() - changed).days
        if age > STALE_DAYS:
            fail(path, f"active for {age} days without a change (limit {STALE_DAYS}): update it, set it deferred, or close it")


def strip_fences(text):
    """Drop fenced code blocks; a block closes only on a fence of the same character and at least the same length."""
    out = []
    opening = None
    for line in text.split("\n"):
        m = FENCE_RE.match(line)
        if opening is None:
            if m:
                opening = m.group("marker")
            else:
                out.append(line)
        elif m and m.group("marker")[0] == opening[0] and len(m.group("marker")) >= len(opening):
            opening = None

    return "\n".join(out)


def is_external(target):
    return bool(re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I)) or target.startswith("//") or target.startswith("#")


def check_links(path):
    base = os.path.dirname(path)
    text = strip_fences(read(path))
    matches = list(LINK_RE.finditer(text)) + list(REF_RE.finditer(text))
    for m in matches:
        target = m.group("angled") if m.group("angled") is not None else m.group("target")
        if is_external(target):
            continue

        rel = target.split("#", 1)[0]
        if not rel or os.path.exists(os.path.normpath(os.path.join(base, rel))):
            continue

        fail(path, f"relative link {target!r} resolves to no file")


def md_files(directory):
    out = []
    for dirpath, _, names in os.walk(directory):
        for n in sorted(names):
            if n.endswith(".md"):
                out.append(os.path.join(dirpath, n))

    return sorted(out)


def check_subject(subject_dir):
    readme = os.path.join(subject_dir, "README.md")
    rows = {}
    for line in read(readme).split("\n"):
        m = ROW_RE.match(line)
        if m and m.group("cls") in VALUES:
            name = m.group("file").strip()
            if name in rows:
                fail(readme, f"row `{name}` appears more than once")
            rows[name] = (m.group("cls"), m.group("status"))

    files = {os.path.relpath(p, subject_dir) for p in md_files(subject_dir)} - {"README.md"}
    for f in sorted(files - rows.keys()):
        fail(readme, f"file `{f}` has no row")
    for f in sorted(rows.keys() - files):
        fail(readme, f"row `{f}` names no file")

    for f in sorted(files & rows.keys()):
        cls, row_status = rows[f]
        path = os.path.join(subject_dir, f)
        if row_status not in VALUES[cls]:
            fail(readme, f"row `{f}`: Status {row_status!r} is not allowed for class {cls!r}")

        value, date = status_of(path)
        if value is not None and value != row_status:
            fail(readme, f"row `{f}` says {row_status!r} but the file says {value!r}")
        if cls == "finite":
            check_finite(path, value, date)

    for path in md_files(subject_dir):
        check_links(path)


def main():
    checked = 0
    if os.path.isfile(ROADMAP):
        check_document(ROADMAP, "living")
        check_links(ROADMAP)
        checked += 1
    else:
        fail(ROADMAP, "missing")

    if os.path.isdir(SPECS):
        for name in sorted(os.listdir(SPECS)):
            subject_dir = os.path.join(SPECS, name)
            if os.path.isfile(os.path.join(subject_dir, "README.md")):
                check_subject(subject_dir)
                checked += 1

    if os.path.isdir(PLANS):
        for name in sorted(os.listdir(PLANS)):
            path = os.path.join(PLANS, name)
            if os.path.isfile(path) and name.endswith(".md"):
                check_document(path, "finite")
                check_links(path)
                checked += 1

    check_active()

    for e in errors:
        print(e, file=sys.stderr)

    if errors:
        print(f"docs-check: {len(errors)} error(s)", file=sys.stderr)
        return 1

    print(f"docs-check: ok ({checked} document set(s) checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
