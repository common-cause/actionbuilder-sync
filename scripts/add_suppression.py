"""
add_suppression.py — add a person to the ActionBuilder suppression list.

Usage:
    python scripts/add_suppression.py \\
        --email someone@example.com --email their.other@example.com \\
        --entity 00000000-0000-0000-0000-000000000000 \\
        --reason "asked to be removed" \\
        --added-by "human:rob"

Each identifier (--email, --person-id, --entity, all repeatable) becomes one row in
actionbuilder_sync.suppression_list. Effect:
  - the remove_suppressed sync op removes matching entities from all active campaigns
    (feed: actionbuilder_sync.suppression_removal)
  - the insert/connect feeds (deduplicated_names_to_load, organizing_team_inserts,
    organizing_team_connects) permanently exclude matching people

The suppression_list table must already exist (run scripts/create_suppression_list.sql once).
"""

import argparse
import sys
from datetime import timezone, datetime

from dotenv import load_dotenv

from ccef_connections.connectors.bigquery import BigQueryConnector
from ccef_connections.exceptions import WriteError

PROJECT = "proj-tmc-mem-com"
TABLE = f"{PROJECT}.actionbuilder_sync.suppression_list"


def parse_args():
    p = argparse.ArgumentParser(description="Add a person to the AB suppression list.")
    p.add_argument("--email", action="append", default=[],
                   help="Email to suppress (repeatable)")
    p.add_argument("--person-id", action="append", default=[],
                   help="core_enhanced person_id to suppress (repeatable)")
    p.add_argument("--entity", action="append", default=[],
                   help="AB entity interact_id to suppress (repeatable)")
    p.add_argument("--reason", required=True, help="Why this person is suppressed")
    p.add_argument("--added-by", default="human:unknown",
                   help="Who is adding: 'human:rob', 'ai:claude-fable-5', etc.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the rows that would be inserted without writing")
    return p.parse_args()


def main():
    args = parse_args()
    now = datetime.now(tz=timezone.utc).isoformat()

    rows = []
    for email in args.email:
        rows.append({"email": email.strip().lower(), "person_id": None,
                     "entity_interact_id": None, "reason": args.reason,
                     "added_by": args.added_by, "added_at": now})
    for pid in args.person_id:
        rows.append({"email": None, "person_id": pid.strip(),
                     "entity_interact_id": None, "reason": args.reason,
                     "added_by": args.added_by, "added_at": now})
    for iid in args.entity:
        if len(iid.strip()) != 36:
            sys.exit(f"ERROR: --entity must be a 36-char interact_id UUID, got {iid!r}")
        rows.append({"email": None, "person_id": None,
                     "entity_interact_id": iid.strip(), "reason": args.reason,
                     "added_by": args.added_by, "added_at": now})

    if not rows:
        sys.exit("ERROR: provide at least one --email / --person-id / --entity")

    if args.dry_run:
        print(f"DRY RUN - would insert {len(rows)} row(s):")
        for r in rows:
            ident = r["email"] or r["person_id"] or r["entity_interact_id"]
            print(f"  {ident}  ({args.reason})")
        return

    load_dotenv(dotenv_path='.env')
    bq = BigQueryConnector(project_id=PROJECT)
    bq.connect()
    try:
        bq.insert_rows(TABLE, rows)
    except WriteError as e:
        sys.exit(f"ERROR inserting rows: {e}")

    print(f"Added {len(rows)} suppression row(s) ({args.added_by}): {args.reason}")
    print("Next steps: run 'python scripts/sync.py remove_suppressed --dry-run' to preview "
          "campaign removals, then without --dry-run to execute. Insert/connect feeds "
          "exclude these identifiers automatically.")


if __name__ == "__main__":
    main()
