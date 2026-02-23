"""
ai_resolve_dedup.py — Feed dedup_unresolved pairs to GPT-4o and get resolution decisions.

Usage:
    python scripts/ai_resolve_dedup.py              # dry run (prints decisions, no writes)
    python scripts/ai_resolve_dedup.py --write       # write decisions to dedup_resolutions
    python scripts/ai_resolve_dedup.py --write --skip-low-confidence  # only write high/medium

Reviews each pair in dedup_unresolved, sends all to GPT-4o with full context,
and returns structured MERGE/KEEP_BOTH/DEFER decisions with reasoning.
MERGE decisions flow into dedup_candidates on the next dbt run.
"""

import argparse
import json
import os
import sys
from datetime import timezone, datetime
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel

# Load env before importing ccef_connections (it needs OPENAI_API_KEY_PASSWORD)
load_dotenv(dotenv_path='.env')

from ccef_connections import OpenAIConnector
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


# ── BigQuery setup ─────────────────────────────────────────────────────────────

PROJECT = "proj-tmc-mem-com"
RESOLUTIONS_TABLE = f"{PROJECT}.actionbuilder_sync.dedup_resolutions"


def get_bq_client():
    cred_json = os.environ.get("BIGQUERY_API_CREDENTIALS_PASSWORD", "")
    if not cred_json:
        sys.exit("ERROR: BIGQUERY_API_CREDENTIALS_PASSWORD not set")
    return bigquery.Client(
        credentials=Credentials.from_service_account_info(json.loads(cred_json)),
        project=PROJECT,
    )


def fetch_unresolved_pairs(client):
    rows = list(client.query(f"""
        SELECT
          pair_id,
          signal_type,
          signal_value,
          entity_a_interact_id,
          entity_a_first_name,
          entity_a_last_name,
          entity_a_email,
          entity_a_phone,
          entity_a_state,
          entity_a_tag_count,
          entity_a_person_id,
          entity_a_voterbase_id,
          entity_a_created_date,
          entity_b_interact_id,
          entity_b_first_name,
          entity_b_last_name,
          entity_b_email,
          entity_b_phone,
          entity_b_state,
          entity_b_tag_count,
          entity_b_person_id,
          entity_b_voterbase_id,
          entity_b_created_date
        FROM `{PROJECT}`.actionbuilder_sync.dedup_unresolved
        ORDER BY signal_type, entity_a_last_name, entity_a_first_name
    """).result())
    return rows


# ── Pydantic response model ────────────────────────────────────────────────────

class PairDecision(BaseModel):
    pair_id: str
    decision: Literal["MERGE_A_INTO_B", "MERGE_B_INTO_A", "KEEP_BOTH", "DEFER"]
    reason: str
    confidence: Literal["high", "medium", "low"]


class DeduplicationDecisions(BaseModel):
    decisions: list[PairDecision]


# ── Prompt ─────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert data quality analyst for Common Cause, a nonpartisan
political advocacy organization. You are reviewing possible duplicate person records in
ActionBuilder, Common Cause's activist organizing tool.

Each pair of records was flagged by one of two signals:

  voterbase_id_diff_name
    Both records resolve to the same TargetSmart voter file entry (via
    email -> identity hub person_id -> voterbase_id), but their names differ.
    Exact-same-name matches were already auto-deleted; these are the ambiguous ones.

  shared_phone_same_lastname
    Both records share a 10-digit phone number and the same last name but reached
    this queue because they didn't match through the email identity chain.
    Common causes: same person with two email registrations, OR household members
    sharing a landline (different first names = different people).

For each pair, decide:

  MERGE_A_INTO_B  — Entity A is DELETED. Entity B SURVIVES.
  MERGE_B_INTO_A  — Entity B is DELETED. Entity A SURVIVES.
  KEEP_BOTH       — Confirmed distinct people (e.g. household members with different first names)
  DEFER           — Genuinely unclear; needs human review

IMPORTANT: Before outputting a decision, state internally: "I want to DELETE ___ and KEEP ___."
Then verify: if you want to delete A, output MERGE_A_INTO_B. If you want to delete B, output MERGE_B_INTO_A.


── STEP 1: IS EITHER NAME CORRUPTED OR UNNATURAL? ─────────────────────────────

Before anything else, assess whether either name looks like a data entry error
rather than a real human name. Corrupted names must be deleted in favor of the
clean one, even if the corrupted entity has more tags.

Corruption patterns to recognize:

  Repeated words
    The same word appears twice in the name string where it shouldn't.
    Examples: "O'Toole O'Toole", "Karen Klauseger Klauseger",
              "Laurine Laurine Cooke", "Robert John John Bennett",
              "Catherine Turcer Turcer"
    → The entity with the repeated word is CORRUPTED. DELETE that entity.
    → Tag count does NOT matter. Even if the corrupted entity has 19 tags and the
      clean one has only 1 tag, the corrupted entity is still deleted.
    → Examples:
        A = "O'Toole O'Toole" (corrupted, 1 tag), B = "James O'Toole" (clean, 1 tag)
          → DELETE A, KEEP B → MERGE_A_INTO_B.
        A = "Catherine Turcer" (clean, 1 tag), B = "Catherine Turcer Turcer" (corrupted, 19 tags)
          → B is corrupted. DELETE B (even though B has more tags). KEEP A → MERGE_B_INTO_A.
          Internal check: "I want to DELETE B and KEEP A."
            Deleting B → MERGE_B_INTO_A. Output: MERGE_B_INTO_A.

  Household concatenation
    Two people's names have been jammed into one record using "&", "And",
    or plain concatenation with no separator.
    Examples: "John & Marianna Connolly", "Richard And Sara Loeppert",
              "Pat Thompson Barbara Brass" (two full names run together with no separator)
    → The concatenated entity is always DELETED. Always. The other entity survives.

    To find the correct MERGE direction, follow this algorithm:
      1. Identify the concatenated entity (the one with two full names in it).
      2. Split it mentally into its component individual names.
         e.g. "Pat Thompson Barbara Brass" → ["Pat Thompson", "Barbara Brass"]
         e.g. "John & Marianna Connolly"   → ["John Connolly", "Marianna Connolly"]
         e.g. "Richard And Sara Loeppert"  → ["Richard Loeppert", "Sara Loeppert"]
      3. Check which component name matches the OTHER entity in the pair.
         e.g. The other entity is "Pat Thompson" → matches component "Pat Thompson"
              → confirmed: the concatenated entity should be DELETED, "Pat Thompson" KEPT.
      4. If no component clearly matches, keep the individual whose name appears
         FIRST in the concatenated string.
      5. Set the MERGE direction to DELETE the concatenated entity.
         If the concatenated entity is A → MERGE_A_INTO_B (A deleted, B kept).
         If the concatenated entity is B → MERGE_B_INTO_A (B deleted, A kept).

    Worked examples:
      A = "Pat Thompson Barbara Brass", B = "Pat Thompson"
        → A is concatenated. Components: Pat Thompson, Barbara Brass.
        → "Pat Thompson" matches B. DELETE A, KEEP B → MERGE_A_INTO_B.

      A = "John & Marianna Connolly", B = "John Connolly"
        → A is concatenated. Components: John Connolly, Marianna Connolly.
        → "John Connolly" matches B. DELETE A, KEEP B → MERGE_A_INTO_B.

      A = "Sara Loeppert", B = "Richard And Sara Loeppert"
        → B is concatenated. Components: Richard Loeppert, Sara Loeppert.
        → "Sara Loeppert" matches A. DELETE B, KEEP A → MERGE_B_INTO_A.

  Initials only
    The first name (or entire name) is just one or two letters with no clear
    given name — not a legitimate short name.
    Examples: "A E", "S H", "M Pratt" (initial + surname only)
    → Initials-only is the WEAKER record. Delete it, keep the full name.
    → Note: "Sam", "Rob", "Dick", "Matt", "Frank" are real short names, not initials.

  Truncated / partial names
    A name appears cut off or incomplete in a way no person would write their own name.
    Examples: "Faith Howell L" (trailing initial suggests the last name was truncated
              mid-hyphen, likely "Howell-Bey" or similar hyphenated surname)
    → Truncated names are the WEAKER record. Delete truncated, keep the complete name.


── STEP 2: IS THIS A NATURAL NAME VARIANT? ─────────────────────────────────────

If neither name is corrupted, consider whether the difference is a legitimate
personal name variation:

  Nicknames and common short forms
    Rob/Robert, Dick/Richard, Matt/Matthew, Frank/Franklyn, Kate/Katherine, etc.
    These are the SAME person. Prefer the entity with more tags or, on a tie,
    the fuller legal name.

  Accented / international characters
    Names like "Michèle" (French), "José", "Renée" are REAL names, not encoding
    errors. Do not treat an accent as corruption. Treat these as name variants of
    the same person and use tag count / phone presence to choose which to keep.

  Middle initials or middle names
    "Mary K Sykes" vs "Mary Sykes", "Robert L. Krause" vs "Robert Krause" —
    same person, keep the entity with more tags.

  Hyphen variants
    "Diane Shuster-Cooper" vs "Diane Shuster Cooper" — same person, same name,
    different punctuation. Keep the entity with more tags or a phone.

  Maiden/married name changes
    Different last names but same first name and same voterbase_id could be a
    name change after marriage or divorce. This is a MERGE case with medium
    confidence unless other signals (completely unrelated emails, different states)
    suggest otherwise.


── STEP 3: WHICH ENTITY TO KEEP ────────────────────────────────────────────────

After deciding to MERGE, apply the following rules IN ORDER. Stop at the first
rule that resolves the tie. Do NOT skip ahead; do NOT apply a later rule because
you "prefer" one name or one email domain.

  RULE 0 — CORRUPTION OVERRIDE:
    If one name is corrupted (repeated word, concatenation, initials-only,
    truncated) and the other is clean → always keep the clean name.
    This overrides tag count, phone presence, and all other rules.

  RULE 1 — SIGNIFICANT TAG ADVANTAGE:
    If one entity has 3 or more tags than the other → keep the entity with more
    tags. Engagement history is costly to lose.

  RULE 2 — PHONE PRESENCE (applies when tags are equal or within 2):
    Check phone values BEFORE comparing names, email domains, or any other field.
    If one entity has a phone number AND the other has phone=None:
      → KEEP the entity with the phone. DELETE the entity with phone=None.
      → This is a hard rule. It overrides ALL of the following:
          - Name quality preferences ("Michèle sounds more authentic than Michele")
          - Accent presence ("the accented version is more accurate")
          - Email domain preferences ("earthlink.com > earthlink.net")
          - Any other subjective assessment
      → EMAIL DOMAIN IS NOT A TIEBREAKER. .com vs .net, .org vs .com — ignore.
      → ACCENT IS NOT A QUALITY INDICATOR. "Michèle" and "Michele" are equally
        valid. Neither is more correct than the other. Do not use accent to break
        a phone tie.

    Worked example (RULE 2 in action):
      A = "Michele Mattingly"  | email: wavewithin@earthlink.net | phone: 6197392319 | tags: 2
      B = "Michèle Mattingly" | email: wavewithin@earthlink.com | phone: None       | tags: 2

      Check Rule 1: tags 2 vs 2 — no advantage. Continue to Rule 2.
      Check Rule 2: A has phone 6197392319. B has phone=None.
        → KEEP A. DELETE B.
        → Output: MERGE_B_INTO_A.
      (Justification: phone rule fires. Email domain (.net vs .com) is irrelevant.
       Accent on "Michèle" is irrelevant. Rule 2 decides the outcome.)

  RULE 3 — REMAINING TIE-BREAKERS (only when both have a phone or both have None):
    a. Work/org email (.org, employer domain) over personal email — for staff records
    b. Fuller legal name over nickname or name with middle initial omitted
    c. Older created_date (longer-established record)


── STEP 4: WHEN TO DEFER ────────────────────────────────────────────────────────

Set decision=DEFER and confidence=low when:
  - Both first AND last names are completely different with no obvious relationship
    (not nicknames, not initials, not truncation) AND you suspect a TargetSmart
    data error rather than the same real person
  - You cannot determine which entity is the individual vs. the duplicate without
    additional information not present in the data

Return one decision per pair_id. Include a concise reason (1-2 sentences) explaining
your logic (especially noting corruption or variant type). Set confidence accordingly."""


def format_pairs_for_prompt(rows) -> str:
    lines = [f"Total pairs to review: {len(rows)}\n"]
    for i, r in enumerate(rows, 1):
        lines.append(f"--- Pair {i} ---")
        lines.append(f"pair_id: {r.pair_id}")
        lines.append(f"signal: {r.signal_type}  value: {r.signal_value}")
        lines.append(
            f"Entity A: {r.entity_a_first_name} {r.entity_a_last_name}"
            f" | email: {r.entity_a_email}"
            f" | phone: {r.entity_a_phone}"
            f" | state: {r.entity_a_state}"
            f" | tags: {r.entity_a_tag_count}"
            f" | created: {r.entity_a_created_date}"
        )
        lines.append(
            f"Entity B: {r.entity_b_first_name} {r.entity_b_last_name}"
            f" | email: {r.entity_b_email}"
            f" | phone: {r.entity_b_phone}"
            f" | state: {r.entity_b_state}"
            f" | tags: {r.entity_b_tag_count}"
            f" | created: {r.entity_b_created_date}"
        )
        lines.append("")
    return "\n".join(lines)


# ── Write resolutions to BQ ────────────────────────────────────────────────────

def write_resolutions(client, pair_rows_by_id, decisions, skip_low_confidence=False):
    rows_to_insert = []
    skipped = []
    now = datetime.now(tz=timezone.utc).isoformat()

    for d in decisions:
        if skip_low_confidence and d.confidence == "low":
            skipped.append(d.pair_id)
            continue

        pair = pair_rows_by_id.get(d.pair_id)
        if not pair:
            print(f"  WARNING: pair_id {d.pair_id!r} not found in fetched data, skipping")
            continue

        if d.decision == "MERGE_A_INTO_B":
            delete_iid = pair.entity_a_interact_id
            keep_iid = pair.entity_b_interact_id
        elif d.decision == "MERGE_B_INTO_A":
            delete_iid = pair.entity_b_interact_id
            keep_iid = pair.entity_a_interact_id
        else:
            delete_iid = None
            keep_iid = None

        rows_to_insert.append({
            "pair_id": d.pair_id,
            "entity_a_interact_id": pair.entity_a_interact_id,
            "entity_b_interact_id": pair.entity_b_interact_id,
            "decision": d.decision,
            "delete_interact_id": delete_iid,
            "keep_interact_id": keep_iid,
            "reason": d.reason,
            "resolved_by": "ai:gpt-4o",
            "resolved_at": now,
        })

    if rows_to_insert:
        errors = client.insert_rows_json(RESOLUTIONS_TABLE, rows_to_insert)
        if errors:
            sys.exit(f"ERROR inserting rows: {errors}")
        print(f"\nWrote {len(rows_to_insert)} resolution(s) to dedup_resolutions.")

    if skipped:
        print(f"Skipped {len(skipped)} low-confidence decision(s) (use without "
              "--skip-low-confidence to include them).")

    return len(rows_to_insert)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AI-resolve dedup_unresolved pairs.")
    parser.add_argument("--write", action="store_true",
                        help="Write decisions to dedup_resolutions (default: dry run)")
    parser.add_argument("--skip-low-confidence", action="store_true",
                        help="Do not write low-confidence decisions")
    parser.add_argument("--model", default="gpt-4o",
                        help="OpenAI model to use (default: gpt-4o)")
    args = parser.parse_args()

    # Fetch pairs
    print("Fetching unresolved pairs from BigQuery...")
    bq = get_bq_client()
    rows = fetch_unresolved_pairs(bq)
    if not rows:
        print("No unresolved pairs found. Nothing to do.")
        return

    print(f"Found {len(rows)} unresolved pairs. Sending to {args.model}...")
    pair_rows_by_id = {r.pair_id: r for r in rows}

    # Build prompt
    user_content = format_pairs_for_prompt(rows)

    # Call GPT
    connector = OpenAIConnector()
    result = connector.invoke_with_structured_output(
        model=args.model,
        system_prompt=SYSTEM_PROMPT,
        user_content=user_content,
        response_model=DeduplicationDecisions,
        temperature=0.1,
    )

    # Display results
    merge_a = [d for d in result.decisions if d.decision == "MERGE_A_INTO_B"]
    merge_b = [d for d in result.decisions if d.decision == "MERGE_B_INTO_A"]
    keep_both = [d for d in result.decisions if d.decision == "KEEP_BOTH"]
    defer = [d for d in result.decisions if d.decision == "DEFER"]

    print(f"\n{'='*60}")
    print(f"GPT-4o decisions on {len(result.decisions)} pairs:")
    print(f"  MERGE_A_INTO_B : {len(merge_a)}")
    print(f"  MERGE_B_INTO_A : {len(merge_b)}")
    print(f"  KEEP_BOTH      : {len(keep_both)}")
    print(f"  DEFER          : {len(defer)}")
    print(f"{'='*60}\n")

    # Print each decision
    for signal_label in ["voterbase_id_diff_name", "shared_phone_same_lastname"]:
        signal_decisions = [
            d for d in result.decisions
            if pair_rows_by_id.get(d.pair_id) and
               pair_rows_by_id[d.pair_id].signal_type == signal_label
        ]
        if not signal_decisions:
            continue
        print(f"\n--- {signal_label.upper()} ({len(signal_decisions)}) ---\n")
        for d in signal_decisions:
            pair = pair_rows_by_id.get(d.pair_id)
            if pair:
                a_name = f"{pair.entity_a_first_name} {pair.entity_a_last_name}"
                b_name = f"{pair.entity_b_first_name} {pair.entity_b_last_name}"
                print(f"  [{d.confidence.upper()}] {d.decision}")
                print(f"    A: {a_name} ({pair.entity_a_email}, tags:{pair.entity_a_tag_count})")
                print(f"    B: {b_name} ({pair.entity_b_email}, tags:{pair.entity_b_tag_count})")
                print(f"    Reason: {d.reason}")
                print()

    # Write if requested
    if args.write:
        print("\nWriting decisions to dedup_resolutions...")
        n = write_resolutions(bq, pair_rows_by_id, result.decisions, args.skip_low_confidence)
        print(f"Done. Run 'bash dbt.sh run -s dedup_candidates deduplicated_names_to_load' "
              f"to propagate MERGE decisions.")
    else:
        total_merge = len(merge_a) + len(merge_b)
        print(f"DRY RUN complete. {total_merge} merges + {len(keep_both)} keep-boths "
              f"+ {len(defer)} defers ready to write.")
        print("Re-run with --write to commit decisions to BigQuery.")


if __name__ == "__main__":
    main()
