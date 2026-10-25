# data/

Real inputs (traffic logs, captures, a deployment's configuration) and everything derived from them.
tracekeep has none yet; the directory exists so that the rule below is in force from the first commit.
Only this file and `.gitignore` are tracked; every other file here stays local.

## Rules

- Keep the tree owner-only: `chmod -R go-rwx data`.
- Nothing read from here enters a tracked file, a commit message or a message to another agent session:
  no company, site, tool ID, person or internal-service name, no local path, no real traffic
  (`.agents/rules/100-overview.md`, prime directive 1).
  A message to another session carries counts, not content, and is sent only with the owner's approval.
- A test never reads from here; its only recorded input is a masked or synthetic pack (`.agents/rules/300-testing.md`).
- `make data-check` fails when git tracks or stages any other file under `data/`;
  `.gitignore` here re-includes only top-level, anchored names.
  `make hooks`, once per clone, runs the same check before every commit.
- `LOCAL.md` is this deployment's index, untracked:
  per input, its source and time span, and the commands that regenerate anything derived from it.
  Update it whenever an input is added.
