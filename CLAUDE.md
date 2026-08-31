# ActionBuilder Sync

Manages BigQuery views (via dbt) that feed people's action participation data into ActionBuilder, Common Cause's organizing CRM. Handles deduplication, tag management, and new record creation. Uses ccef-connections for BQ access.

## PII / Data Handling

Row-level PII (names, emails, phones, street addresses, gift amounts) **never gets
committed to git** — repos here are org-visible via shared corpora and export pipelines.
Any directory that will receive raw dumps or query results gets gitignored BEFORE the
first file lands (allowlist known-clean file types; never enumerate known-bad files).
Committed derivatives must be masked or aggregated; fabricate example rows in docs.
Row-level people-data lives in access-controlled systems (BigQuery, ROI, Action Network,
shared Sheets) — point at it, don't copy it. Full policy: knowledge library entry
`pii-handling-policy` (`kl_get`).

## Agent Automation & Dispatch

Two different mechanisms. Picking the wrong one wastes the build:

- **Deterministic pipeline → Civis.** Plain Python/dbt ETL, no judgment; tracked in
  this project's `civis/SCHEDULED_SCRIPTS.md`.
- **Judgment pass → local scheduled agent, via a dispatch contract.** Anything whose
  correctness depends on a rubric, world knowledge, or a call a human would otherwise
  make. Subscription Claude Code **cannot be invoked from Civis at all** — no API-key
  path there uses the subscription — so "a Civis job that exercises judgment" is
  unbuildable, not merely discouraged. Don't start building one.

Agent-dispatchable work is governed by the **Dispatch Treaty** (ratified 2026-08-20,
in force since 2026-08-25; law: meta-project `docs/dispatch_treaty.md`). The
rob-assistant "tower" spawns headless agents at named task types that a project
declares in a committed contract. Live fleet status — who has declared what, and what
is actually granted — is the meta-project's generated `dispatch/roster.yaml`; don't
trust a count written in prose anywhere, including here.

**To make a task type in this project dispatchable:**

1. Write `.claude/dispatch.yaml` from the meta-project's `templates/dispatch.yaml`
   (one file, all of this project's task types). **Absence of that file means
   hands-off** — eligibility is declared, never inferred, and no stub is wanted for
   an interactive-only project.
2. Package the procedure itself as the runbook the contract points at — a skill at
   `.claude/skills/<name>/SKILL.md`, or a doc under `docs/`.
3. Confirm **git can see the contract.** A blanket `.claude/*` gitignore swallows it
   silently; add `!.claude/dispatch.yaml`. A contract git can't see does not exist.
4. Validate from the meta-project: `python sync_projects.py --check`, then
   `--dispatch-roster`.
5. **Stop there.** Tiers are dated grants that live only in the meta catalog
   (`projects_index.yaml`), and **only Rob grants one** — an agent proposes, never
   self-authorizes. An ungranted contract is the correct resting state: the roster
   computes `dispatchable: false` and nothing fires.

Do not register a Windows Task Scheduler job for an agent pass either — scheduled
fires go through the tower, or they earn no track record. Background and the
scheduler mechanics: knowledge library entries `dispatch-treaty-and-the-tower` and
`local-scheduled-claude-agents-task-scheduler-the-pattern-for-recurring-agentic-p`
(`kl_get`).
