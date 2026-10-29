# Production admission guide

Status: current (2026-10-30) — the policy, its staging, the masking migration and the cutover's prerequisites

How a tool type is admitted to production traffic, and what must hold before sensitive traffic reaches tracekeep at all.
Read before a tool type's first production capture, before writing a site's policy, and before the cutover to masking.
The design is `docs/specs/tracekeep/masking-keys.md` §9 and `rollouts.md` §6.4; the commands are `operations.md` §10.4's.

## What protects a recipe

Masking hides only what the policy lists:
a message the policy does not name is stored as it was received.
The baseline policy covers the recipe carriers the SEMI standards define
(process programs in S7, data sets in S13, recipes in S15 and S16, S6F25),
and `tracekeep admin policy show baseline 1` prints its rules.
A tool type is admitted to production only after its manual's recipe carriers are checked against the policy in force,
and an operator procedure, not code, does that check.

## Admitting a tool type

1. From the tool type's manual, list every message that carries recipe content:
   the standard ones above, and any vendor stream or function that carries process parameters, recipe bodies or files.
2. Check each against the policy: the same stream and function, and a path that selects the content.
   A path is 1-based element indexes into nested lists, `*` for every element of a list.
3. Weigh the candidates the baseline leaves off for each site:
   terminal text (S10F1, S10F3, S10F5, S10F9), ATTRDATA (S14F1 to S14F4), CPVAL and CEPVAL (S2F41, S2F49).
4. When anything is missing, write a site policy: the baseline's rules, which it must keep, and the added ones, under a new id or version.

```yaml
policy_id: site
version: 1
item_rules:
  # ... every rule of the baseline (policy show baseline 1) ...
  - {stream: 2, function: 41, path: "2/*/2", domain: cpval}
annotation_rules:
  - {kind: unparsed-entry, domain: source-text}
  - {kind: skipped-bytes, domain: source-text}
  - {kind: unrecognised-line, domain: source-text}
```

5. Stage it: `tracekeep admin config stage <digest key id> <encryption key id> --policy site.yaml`.
   Outside `tracekeep dev` a policy lacking a baseline rule is refused,
   and a policy version, once stored, never changes: a changed policy is a new version.
   The file is read by the process that runs the command, the service's when the command goes through its API.
6. Record which tool types were checked against which policy version, outside the repository.

## The cutover to masking

A capture keeps the configuration it was bound with for its whole life,
so masking applies only to captures admitted after a masking configuration is active.
Before sensitive traffic starts:

- every ingest replica and every compactor is upgraded at the migration of the release that masks:
  `tracekeep migrate` moves the membership tables and the compactor lease (T-43),
  so an earlier ingest replica can no longer register or renew and stops fetching,
  and an earlier compactor can no longer hold the lease and stops compacting;
  the traffic waits on the stream meanwhile, nothing is lost, and the upgraded replicas take it up;
- the masking configuration is staged (above) and activated with `tracekeep admin config activate <config_id>`;
  outside `tracekeep dev` a capture is admitted only under a masking configuration whose policy holds the whole baseline,
  so until one is active new captures wait as `incompatible-configuration`, and the alert says so;
- every producer whose capture predates the cutover is stopped or rotated, old leaders that may still publish included,
  since such a capture keeps its policy-less configuration;
- the new captures are confirmed bound to the masking configuration: `config list` names its id, and `ListCaptures` each capture's.

Captures admitted before the cutover, and their backlog, are still processed under their own configuration;
they hold only traffic that was never sensitive.
