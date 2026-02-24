"""
add_resolution.py — record a resolution decision for a dedup_unresolved pair.

Usage:
    python scripts/add_resolution.py \\
        --pair-id "UUID1:UUID2" \\
        --decision MERGE_A_INTO_B \\
        --reason "Same person, entity A has fewer tags" \\
        --resolved-by "human:rob"

For MERGE decisions, --delete and --keep are inferred from --decision and the pair components.
For KEEP_BOTH or DEFER, --delete/--keep are omitted.

Decisions:
    MERGE_A_INTO_B   delete entity_a (first UUID), keep entity_b
    MERGE_B_INTO_A   delete entity_b (second UUID), keep entity_a
    KEEP_BOTH        confirmed distinct people; removes pair from unresolved queue
    DEFER            needs more info; pair stays in unresolved queue

The dedup_resolutions table must already exist (run scripts/create_dedup_resolutions.sql once).
"""

import argparse
import sys
from datetime import timezone, datetime

from dotenv import load_dotenv

from ccef_connections.connectors.bigquery import BigQueryConnector
from ccef_connections.exceptions import WriteError

DATASET = "actionbuilder_sync"
TABLE = "dedup_resolutions"
PROJECT = "proj-tmc-mem-com"
VALID_DECISIONS = {"MERGE_A_INTO_B", "MERGE_B_INTO_A", "KEEP_BOTH", "DEFER"}


def get_bq_client() -> BigQueryConnector:
    load_dotenv(dotenv_path='.env')
    bq = BigQueryConnector(project_id=PROJECT)
    bq.connect()
    return bq


def parse_args():
    p = argparse.ArgumentParser(description="Record a dedup resolution decision.")
    p.add_argument("--pair-id", required=True,
                   help="Canonical pair ID from dedup_unresolved (UUID_A:UUID_B)")
    p.add_argument("--decision", required=True, choices=sorted(VALID_DECISIONS),
                   help="Resolution decision")
    p.add_argument("--reason", default=None,
                   help="Human-readable rationale (optional but recommended)")
    p.add_argument("--resolved-by", default="human:unknown",
                   help="Who is resolving: 'human:rob', 'ai:claude-sonnet-4-6', etc.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the row that would be inserted without writing to BQ")
    return p.parse_args()


def main():
    args = parse_args()

    # Parse pair_id into entity_a / entity_b
    parts = args.pair_id.split(":")
    if len(parts) != 2 or not all(len(p) == 36 for p in parts):
        sys.exit(
            f"ERROR: --pair-id must be two 36-char UUIDs separated by ':'\n"
            f"       Got: {args.pair_id!r}"
        )
    entity_a_iid, entity_b_iid = parts

    # Derive delete/keep interact_ids for MERGE decisions
    decision = args.decision
    if decision == "MERGE_A_INTO_B":
        delete_iid = entity_a_iid
        keep_iid = entity_b_iid
    elif decision == "MERGE_B_INTO_A":
        delete_iid = entity_b_iid
        keep_iid = entity_a_iid
    else:
        delete_iid = None
        keep_iid = None

    row = {
        "pair_id": args.pair_id,
        "entity_a_interact_id": entity_a_iid,
        "entity_b_interact_id": entity_b_iid,
        "decision": decision,
        "delete_interact_id": delete_iid,
        "keep_interact_id": keep_iid,
        "reason": args.reason,
        "resolved_by": args.resolved_by,
        "resolved_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    if args.dry_run:
        print("DRY RUN - would insert:")
        for k, v in row.items():
            print(f"  {k}: {v!r}")
        return

    client = get_bq_client()
    table_ref = f"{PROJECT}.{DATASET}.{TABLE}"

    # Check for existing resolution for this pair_id
    # pair_id is validated above to be two 36-char UUIDs — safe to inline
    check_query = f"""
        SELECT pair_id, decision, resolved_by, resolved_at
        FROM `{table_ref}`
        WHERE pair_id = '{args.pair_id}'
    """
    existing = list(client.query(check_query))
    if existing:
        r = existing[0]
        print(
            f"WARNING: pair_id {args.pair_id!r} already has a resolution:\n"
            f"  decision={r.decision!r}, resolved_by={r.resolved_by!r}, "
            f"resolved_at={r.resolved_at}\n"
            f"Inserting a new row anyway. On next dbt run, the first MERGE decision "
            f"for this pair_id wins (de-duped by dedup_candidates QUALIFY)."
        )

    try:
        client.insert_rows(table_ref, [row])
    except WriteError as e:
        sys.exit(f"ERROR inserting row: {e}")

    print(f"Recorded {decision} for pair {args.pair_id}")
    if delete_iid:
        print(f"  delete: {delete_iid}")
        print(f"  keep:   {keep_iid}")
    print(f"  resolved_by: {args.resolved_by}")
    if args.reason:
        print(f"  reason: {args.reason}")


if __name__ == "__main__":
    main()
