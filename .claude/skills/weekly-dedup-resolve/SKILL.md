---
name: weekly-dedup-resolve
description: Weekly unattended pass — resolve new dedup_unresolved pairs (ActionBuilder duplicate candidates) with agent judgment over enriched evidence, writing MERGE/KEEP_BOTH decisions to dedup_resolutions. Run headless by Task Scheduler via scripts/run_weekly_dedup.ps1, or interactively with /weekly-dedup-resolve.
---

# Weekly dedup resolution (agent judgment pass)

You are resolving possible duplicate person records in ActionBuilder for Common Cause.
Each pair was flagged by `voterbase_id_diff_name` (same voter-file entry, different
names) or `shared_phone_same_lastname`. Exact-name duplicates were auto-handled
upstream; every pair here needs judgment.

## Procedure

1. `python scripts/agent_resolve_dedup.py evidence`
   - `PAIRS: 0` → report "queue empty" and STOP.
   - `PAIRS:` more than 40 → do NOT resolve anything. Report "ABORT: N pairs —
     likely an upstream bulk event; needs human review" and STOP. (Normal weekly
     volume is 0–15.)
2. Judge EVERY pair with the rubric below. Produce a decision (or deliberately skip).
3. Write the decisions array to `logs/dedup_decisions_<YYYY-MM-DD>.json` — objects:
   `{"pair_id", "entity_a_interact_id", "entity_b_interact_id", "decision",
   "confidence", "reason"}`. Include ONLY high/medium-confidence decisions; OMIT
   pairs you are unsure about (they stay in the queue for Rob — never write DEFER).
4. `python scripts/agent_resolve_dedup.py write --file logs/dedup_decisions_<date>.json`
5. Report: pairs seen / merges / keep-boths / left in queue, plus one line per
   left-in-queue pair saying why. NEVER run removal ops (remove_records etc.) —
   merges execute later in the human-supervised pass.

## Rubric

**Households first.** The dominant real-world pattern is a COUPLE sharing a landline,
an address, or even an email — email is NOT an individual-level identifier here.
- Clearly different first names (Karen/John, Elaine/William) at one address →
  `KEEP_BOTH` (high), even when they share a phone or a voterbase_id, and even when
  one record carries the other's email pattern (note the contamination in the reason).
- Sr/Jr suffix pairs → KEEP_BOTH unless evidence shows a single person.
- Confirmed doctrine (Rob, 2026-07-30): shared-email couples are KEEP_BOTH.

**Same person if:** first names are variants (Judy/Judith, Tony/Anthony, Rob/Robinson,
Dov/David), initials or truncations of the same name ("A B" = Alexis Barksdale,
"Emily B"), name reversal or corruption (see below), maiden/married surname change at
the same address, or the emails share a distinctive local part (spunk816@gmail vs
spunk816@me; jvbethel@me vs @mac).

**Corruption patterns (delete the corrupted record, regardless of tag counts):**
repeated words ("Nadeau Nadeau"), household concatenations ("Marc And Alice ..."),
initials-only, truncated names, junk suffixes ("Robert Reid is in"), reversed
surnames ("Sioramed" = "Demarois" backwards), garbage characters in the email.
Tag loss is acceptable: contact migration + the nightly sync recompute participation
tags on the keeper. (Only universal OFP competencies do not recompute — check the
keeper is not losing one; it almost never is.)

**Choosing the keeper (in order):** 1) clean name over corrupted — always;
2) 3+ tag advantage; 3) phone present vs absent; 4) durable real email over an
iCloud/duck relay; 5) org email for staff records; 6) fuller legal name;
7) older created date. If the twins are byte-identical, keep entity A.

**Entities marked [ALREADY REMOVED]:** the pair is historical — record the merge
matching reality (removed twin = delete side), confidence high.

**Never merge** when names are genuinely different people, when you suspect a
TargetSmart voterbase collision (different towns, addresses, phones AND names), or
when suppressed/staff records are involved in a way that needs Rob — leave those in
the queue and say why in the report.

## Reporting

End with a compact summary the log can keep: date, pairs seen, written (merge /
keep-both split), left in queue with one-line reasons, and any anomalies (e.g.
unexpectedly high volume, entities of staff, contamination patterns worth a UI fix).
