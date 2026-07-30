"""
agent_resolve_dedup.py — helper for the weekly LOCAL agent dedup-resolution pass.

Two subcommands, used by the weekly-dedup-resolve skill (agent judgment happens in
the Claude session between the two calls — this script is deliberately judgment-free):

    python scripts/agent_resolve_dedup.py evidence
        Prints every dedup_unresolved pair as a numbered evidence block with
        ENRICHED context per entity: all emails, all phones, street address,
        campaign memberships, live tag count, last activity date, and whether the
        entity was already removed per sync_log. Exit code 0 always; prints
        "PAIRS: 0" when the queue is empty.

    python scripts/agent_resolve_dedup.py write --file <decisions.json>
        Writes agent decisions to actionbuilder_sync.dedup_resolutions.
        decisions.json is a JSON array of objects:
          {"pair_id": ..., "entity_a_interact_id": ..., "entity_b_interact_id": ...,
           "decision": "MERGE_A_INTO_B|MERGE_B_INTO_A|KEEP_BOTH",
           "confidence": "high|medium", "reason": "..."}
        Guardrails: DEFER and low-confidence rows are REJECTED (leave those pairs
        out of the file entirely — they stay in the queue); pairs already resolved
        are skipped with a warning; decision vocabulary and interact_ids are
        validated against the live queue before anything is written.

MERGE decisions flow into dedup_candidates (resolved_merge tier) and are executed
later by the human-supervised removal pass (prepare_email_data / prepare_phone_data /
remove_records) — this script never removes anything.
"""

import argparse
import json
import sys
from datetime import timezone, datetime

from dotenv import load_dotenv

from ccef_connections.connectors.bigquery import BigQueryConnector
from ccef_connections.exceptions import WriteError

PROJECT = "proj-tmc-mem-com"
RESOLUTIONS_TABLE = f"{PROJECT}.actionbuilder_sync.dedup_resolutions"
VALID_DECISIONS = {"MERGE_A_INTO_B", "MERGE_B_INTO_A", "KEEP_BOTH"}
VALID_CONFIDENCE = {"high", "medium"}

EVIDENCE_SQL = """
WITH pairs AS (
  SELECT * FROM `proj-tmc-mem-com.actionbuilder_sync.dedup_unresolved`
),
ids AS (
  SELECT entity_a_interact_id AS iid FROM pairs
  UNION DISTINCT
  SELECT entity_b_interact_id FROM pairs
),
ent AS (
  SELECT e.interact_id, e.id
  FROM `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__entities` e
  JOIN ids ON ids.iid = e.interact_id
),
emails AS (
  SELECT en.interact_id, STRING_AGG(DISTINCT LOWER(TRIM(em.email)), ' ; ') AS all_emails
  FROM ent en
  JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__emails` em
    ON em.owner_id = en.id AND em.owner_type = 'Entity'
  WHERE em.email IS NOT NULL
  GROUP BY 1
),
phones AS (
  SELECT en.interact_id, STRING_AGG(DISTINCT ph.number, ' ; ') AS all_phones
  FROM ent en
  JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__phone_numbers` ph
    ON ph.owner_id = en.id AND ph.owner_type = 'Entity'
  WHERE ph.number IS NOT NULL
  GROUP BY 1
),
addr AS (
  SELECT en.interact_id,
         ANY_VALUE(CONCAT(IFNULL(a.street_address,''), ', ', IFNULL(a.city,''), ' ',
                          IFNULL(a.state,''), ' ', IFNULL(a.postal_code,''))) AS address
  FROM ent en
  JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__addresses` a
    ON a.owner_id = en.id AND a.owner_type = 'Entity'
  GROUP BY 1
),
camps AS (
  SELECT en.interact_id,
         STRING_AGG(CAST(ce.campaign_id AS STRING), '+' ORDER BY ce.campaign_id) AS campaigns
  FROM ent en
  JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__campaigns_entities` ce
    ON ce.entity_id = en.id
  GROUP BY 1
),
activity AS (
  SELECT en.interact_id, MAX(DATE(tl.created_at)) AS last_tag_date
  FROM ent en
  JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__taggable_logbook` tl
    ON tl.taggable_id = en.id AND tl.taggable_type = 'Entity' AND tl.deleted_at IS NULL
  GROUP BY 1
),
removed AS (
  SELECT DISTINCT entity_interact_id
  FROM `proj-tmc-mem-com.actionbuilder_sync.removed_campaign_entities`
)
SELECT
  p.pair_id, p.signal_type, p.signal_value,
  p.entity_a_interact_id, p.entity_a_first_name, p.entity_a_last_name,
  p.entity_a_tag_count, CAST(p.entity_a_created_date AS STRING) AS a_created,
  ea.all_emails AS a_all_emails, pa.all_phones AS a_all_phones,
  aa.address AS a_address, ca.campaigns AS a_campaigns,
  CAST(ta.last_tag_date AS STRING) AS a_last_tag,
  (ra.entity_interact_id IS NOT NULL) AS a_removed,
  p.entity_b_interact_id, p.entity_b_first_name, p.entity_b_last_name,
  p.entity_b_tag_count, CAST(p.entity_b_created_date AS STRING) AS b_created,
  eb.all_emails AS b_all_emails, pb.all_phones AS b_all_phones,
  ab.address AS b_address, cb.campaigns AS b_campaigns,
  CAST(tb.last_tag_date AS STRING) AS b_last_tag,
  (rb.entity_interact_id IS NOT NULL) AS b_removed
FROM pairs p
LEFT JOIN emails ea ON ea.interact_id = p.entity_a_interact_id
LEFT JOIN emails eb ON eb.interact_id = p.entity_b_interact_id
LEFT JOIN phones pa ON pa.interact_id = p.entity_a_interact_id
LEFT JOIN phones pb ON pb.interact_id = p.entity_b_interact_id
LEFT JOIN addr aa ON aa.interact_id = p.entity_a_interact_id
LEFT JOIN addr ab ON ab.interact_id = p.entity_b_interact_id
LEFT JOIN camps ca ON ca.interact_id = p.entity_a_interact_id
LEFT JOIN camps cb ON cb.interact_id = p.entity_b_interact_id
LEFT JOIN activity ta ON ta.interact_id = p.entity_a_interact_id
LEFT JOIN activity tb ON tb.interact_id = p.entity_b_interact_id
LEFT JOIN removed ra ON ra.entity_interact_id = p.entity_a_interact_id
LEFT JOIN removed rb ON rb.entity_interact_id = p.entity_b_interact_id
ORDER BY p.signal_type, p.entity_a_last_name, p.entity_a_first_name
"""


def get_bq() -> BigQueryConnector:
    load_dotenv(dotenv_path='.env')
    bq = BigQueryConnector(project_id=PROJECT)
    bq.connect()
    return bq


def _entity_block(r, p: str) -> str:
    name = f"{r[f'entity_{p}_first_name']} {r[f'entity_{p}_last_name']}"
    removed = ' [ALREADY REMOVED]' if r[f'{p}_removed'] else ''
    addr = (r[f'{p}_address'] or '-').strip().rstrip(',')
    return (
        f"{p.upper()}: {name}{removed}\n"
        f"   interact_id: {r[f'entity_{p}_interact_id']}\n"
        f"   emails: {r[f'{p}_all_emails'] or '-'} | phones: {r[f'{p}_all_phones'] or '-'}\n"
        f"   addr: {addr} | campaigns: {r[f'{p}_campaigns'] or '-'} | "
        f"tags: {r[f'entity_{p}_tag_count']} | created: {r[f'{p}_created']} | "
        f"last activity: {r[f'{p}_last_tag'] or '-'}"
    )


def cmd_evidence() -> None:
    bq = get_bq()
    rows = list(bq.query(EVIDENCE_SQL))
    print(f"PAIRS: {len(rows)}")
    for i, r in enumerate(rows, 1):
        sig = 'VB' if r['signal_type'] == 'voterbase_id_diff_name' else 'PH'
        print(f"\n#{i:03d} [{sig}] signal_value={r['signal_value']}")
        print(f"pair_id: {r['pair_id']}")
        print(_entity_block(r, 'a'))
        print(_entity_block(r, 'b'))


def cmd_write(path: str) -> None:
    with open(path, encoding='utf-8') as f:
        decisions = json.load(f)
    if not isinstance(decisions, list):
        sys.exit("ERROR: decisions file must be a JSON array")

    errors = []
    for i, d in enumerate(decisions):
        if d.get('decision') not in VALID_DECISIONS:
            errors.append(f"row {i}: invalid decision {d.get('decision')!r} "
                          f"(DEFER is not writable — omit the pair instead)")
        if d.get('confidence') not in VALID_CONFIDENCE:
            errors.append(f"row {i}: invalid confidence {d.get('confidence')!r} "
                          f"(low-confidence pairs must be omitted, not written)")
        for k in ('pair_id', 'entity_a_interact_id', 'entity_b_interact_id', 'reason'):
            if not d.get(k):
                errors.append(f"row {i}: missing {k}")
    if errors:
        sys.exit("ERROR — nothing written:\n  " + "\n  ".join(errors))

    bq = get_bq()
    live = {
        r['pair_id']: (r['entity_a_interact_id'], r['entity_b_interact_id'])
        for r in bq.query(
            "SELECT pair_id, entity_a_interact_id, entity_b_interact_id "
            "FROM `proj-tmc-mem-com.actionbuilder_sync.dedup_unresolved`"
        )
    }

    now = datetime.now(tz=timezone.utc).isoformat()
    to_insert, skipped = [], []
    for d in decisions:
        pid = d['pair_id']
        if pid not in live:
            skipped.append(pid)
            continue
        if (d['entity_a_interact_id'], d['entity_b_interact_id']) != live[pid]:
            sys.exit(f"ERROR: entity ids for pair {pid} do not match the live queue — "
                     f"nothing written")
        if d['decision'] == 'MERGE_A_INTO_B':
            delete_iid, keep_iid = d['entity_a_interact_id'], d['entity_b_interact_id']
        elif d['decision'] == 'MERGE_B_INTO_A':
            delete_iid, keep_iid = d['entity_b_interact_id'], d['entity_a_interact_id']
        else:
            delete_iid = keep_iid = None
        to_insert.append({
            'pair_id': pid,
            'entity_a_interact_id': d['entity_a_interact_id'],
            'entity_b_interact_id': d['entity_b_interact_id'],
            'decision': d['decision'],
            'delete_interact_id': delete_iid,
            'keep_interact_id': keep_iid,
            'reason': f"[{d['confidence']}] {d['reason']}",
            'resolved_by': 'ai:claude-fable-5 (weekly local agent)',
            'resolved_at': now,
        })

    if skipped:
        print(f"WARNING: {len(skipped)} pair(s) no longer in the queue, skipped: "
              + ", ".join(skipped))
    if not to_insert:
        print("Nothing to write.")
        return
    try:
        bq.insert_rows(RESOLUTIONS_TABLE, to_insert)
    except WriteError as e:
        sys.exit(f"ERROR inserting rows: {e}")
    merges = sum(1 for r in to_insert if r['delete_interact_id'])
    print(f"Wrote {len(to_insert)} resolution(s): {merges} MERGE, "
          f"{len(to_insert) - merges} KEEP_BOTH")


def main() -> None:
    p = argparse.ArgumentParser(description="Weekly agent dedup-resolution helper.")
    sub = p.add_subparsers(dest='cmd', required=True)
    sub.add_parser('evidence', help='Print enriched evidence for all unresolved pairs')
    w = sub.add_parser('write', help='Write agent decisions to dedup_resolutions')
    w.add_argument('--file', required=True, help='Path to decisions JSON array')
    args = p.parse_args()
    if args.cmd == 'evidence':
        cmd_evidence()
    else:
        cmd_write(args.file)


if __name__ == '__main__':
    main()
