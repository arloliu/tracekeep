# 450 — Document lifecycle

Adopted from veq, which adopted it from go-secs:
the additions marked "veq" came with it, those marked "tracekeep" are this repository's.

Every Markdown document under `docs/` is **living** or **finite**, and its class decides how it ends.
Decide the class when you create the file.

- **Living**: in force until replaced — specs, guides, changelogs, decision logs, the roadmap.
  Edit it in place; its changelog or git history carries the past.
- **Finite**: exists to get one piece of work done — plans, proposals, reports (research, audit, root cause).
  It ends by **closing**, and closing deletes the file.

## Where documents live

- `docs/ROADMAP.md` — living; the milestones and the single index of finite documents that are `active` (veq).
- `docs/specs/<subject>/` — one directory per subject:
  its living documents, its finite documents, and a `README.md` index.
  tracekeep has one subject, `tracekeep` (tracekeep).
- `docs/plans/<slug>.md` — a finite plan that belongs to no subject directory.
  When the work also needs a report (a root-cause analysis, a measurement), the report is a section of the plan.
- `docs/guides/<slug>.md` — living how-to documents.

## Names

- File and directory names are kebab-case subjects and carry no dates; the date lives on the Status line.
- A series takes a stable ID: proposals are `proposals/p<N>-<slug>.md`.
  IDs are never reused; the subject `README.md` records the next free one, because closed proposals leave no file behind.

## Status line

Line 3 of every document, after the title and one blank line:

```
Status: <value> (<YYYY-MM-DD>) — <optional clause>
```

| Class | Values |
|---|---|
| living | `draft` (written, not yet in force), `current` (in force) |
| finite | `draft` (awaiting the owner), `active` (approved; work under way), `deferred` (parked; kept as the record a later design starts from) |

The date is the day the value last changed.
The clause is one short clause, never a progress log; the whole line stays within 160 characters (veq).
History goes to a changelog, the decision log or git, not the Status line.
A subject `README.md` is the index and carries no Status line.

Below its Status line, a spec file opens with what it covers and when an agent should read it,
so an agent can decide from its first lines whether the file concerns the task (tracekeep).

## What a plan holds (veq)

A plan says what is to be done and where it stands — nothing else.
Design content belongs in the subject's specs and rationale in its decision log,
so closing a plan loses nothing and nobody keeps one open to preserve what it says.

- A phase table with `pending`, `in-progress` or `done` per phase.
- Only the phase that is `in-progress` (or next) is detailed;
  later phases are one row each and are detailed when they start.
- A `done` row names the commit or pull request that finished it (a hash of 7+ hex digits or `#<n>`).

## Closing a finite document

A finite document closes when its work is done, rejected, withdrawn or superseded.
In the commit that closes it:

1. Record the outcome in the living document that owns it:
   the subject's changelog or decision log, or, for a `docs/plans/` file, the commit message.
   Name the document and where its text lives, e.g. "P3 (object lifecycle) applied as v2.7; last text at `<commit>`".
2. Delete the file.
3. Remove its row from the subject `README.md` and its link from `docs/ROADMAP.md`.

A document is closed when its file is gone and its outcome line exists.

## Subject README

`docs/specs/<subject>/README.md` is the subject's single index:
one row per file with its class, Status value and a one-line purpose, plus the next free proposal ID.
Change it in the same commit as any Status change, addition or deletion.
Other documents in the subject point at the README instead of keeping their own list of files.

## Links and decision logs

- Link only to files in the repository.
  Name material kept outside it — review reports, research notes, archived revisions — in plain text, without a path:
  "P3 review round 3 (kept outside the repository)".
  Such material is never committed, not even under `tmp/` or `docs/` (tracekeep).
- `<subject>-decisions.md` is append-only.
  A superseded entry keeps its text and gains "(superseded by <ID>)".
  A subject's owner decisions go to its decision log.
- tracekeep's decisions are `T-<N>` in `docs/specs/tracekeep/tracekeep-decisions.md`, never reused (tracekeep).
  Rulings of other repositories keep their own IDs (`G5-<N>` in go-secs, `V-<N>` in veq) and are cited, never copied.

## Check

`make docs-check` covers `docs/ROADMAP.md`, every `docs/specs/<subject>/` that has a `README.md`, and every `docs/plans/*.md`.
It fails when:

- a document other than `README.md` lacks the line-3 Status line,
  its value is outside its class (living: `draft`, `current`; finite: `draft`, `active`, `deferred`),
  or the line is longer than 160 characters (veq);
- the README's rows and the directory's files differ, a row's Status differs from the file's, or a row's Class does not allow that value;
  a `docs/plans/` file is finite;
- a relative Markdown link resolves to no file;
- an `active` finite document is not linked from `docs/ROADMAP.md` (veq);
- an `active` finite document has not changed for more than 21 days,
  judged by the later of its Status date and its last commit (veq):
  update it, set it `deferred`, or close it;
- a phase-table row marked `done` names no commit or pull request (veq).

It runs in `make ci`.
