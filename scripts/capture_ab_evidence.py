"""capture_ab_evidence.py - Capture evidence of AB/BQ mirror staleness for bug report.

ActionBuilder's internal SQL mirror stalled at 2026-03-05. This one-time script
produces a JSON evidence file documenting the discrepancy for the AB support ticket.

Three evidence sections:

  A. Deleted taggings still active in mirror
     Query BQ for taggings we know we deleted (via cleanup_duplicate_tags run on
     2026-03-12) that still appear in taggable_logbook with deleted_at IS NULL.
     Confirm the AB API returns 404 for the same tagging_interact_ids (proving they
     are absent from AB but the mirror doesn't reflect the deletion).

  B. New tag writes not appearing in mirror (framework)
     After running update_records with sync_log instrumented, query sync_log for
     add_tagging entries, then check taggable_logbook for corresponding new rows.
     Run this section the day after an update_records run to detect the gap.

  C. Global notes rendering failure
     Call list_person_taggings for a known entity and record the full response.
     API shows a numeric value for 'Phone Bank Calls Made' while the AB UI shows
     'Add a number for this response' — API and UI disagree within AB's own systems.

Usage:
    python scripts/capture_ab_evidence.py [--sections A,B,C]

    --sections   Comma-separated list of sections to run (default: A,C).
                 Section B requires a prior update_records run with sync_log active.
    --entity     Entity interact_id for Sections A and C (default: Ciro Amador CA).

Output:
    evidence/ab_bug_report_{YYYY-MM-DD}.json

Credentials (in .env):
    BIGQUERY_CREDENTIALS_PASSWORD        Service account JSON
    ACTION_BUILDER_CREDENTIALS_PASSWORD  JSON: {"api_token": "...", "subdomain": "..."}
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from ccef_connections.connectors.action_builder import ActionBuilderConnector
from ccef_connections.connectors.bigquery import BigQueryConnector

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BQ_PROJECT = 'proj-tmc-mem-com'

# Ciro Amador, California — the known test case for the mirror staleness bug.
# Internal entity id 5420; interact_id confirmed via BQ lookup in Section A.
DEFAULT_ENTITY_ID_PREFIX = 'a78c68e7'   # first 8 chars; full UUID resolved from BQ
CALIFORNIA_CAMPAIGN_ID = 'fd65be58-cce6-400f-97f8-e14adb6558d3'

# Mirror stalled at this date; operations after this date should not appear in BQ.
MIRROR_STALL_DATE = '2026-03-05'

# Our cleanup run that deleted duplicates — used for Section A evidence baseline.
CLEANUP_RUN_DATE = '2026-03-12'


# ---------------------------------------------------------------------------
# Client helpers
# ---------------------------------------------------------------------------

def _make_bq_client() -> BigQueryConnector:
    bq = BigQueryConnector(project_id=BQ_PROJECT)
    bq.connect()
    return bq


def _make_ab_client() -> ActionBuilderConnector:
    ab = ActionBuilderConnector()
    ab.connect()
    return ab


def _query(bq: BigQueryConnector, sql: str) -> List[Dict[str, Any]]:
    rows = list(bq.query(sql))
    return [dict(r) for r in rows]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Section A: Deleted taggings still active in mirror
# ---------------------------------------------------------------------------

def capture_section_a(
    bq: BigQueryConnector,
    ab: ActionBuilderConnector,
    entity_id: str,
    campaign_id: str,
) -> Dict[str, Any]:
    """
    For the known entity, find all tagging_interact_ids that:
      - still appear in taggable_logbook with deleted_at IS NULL
      - were identified as rn > 1 duplicates (i.e., would have been deleted by cleanup)
      - were written/updated before the mirror stalled at 2026-03-05

    Then call list_person_taggings via the AB API to confirm those taggings
    are absent (404 / not in response), while BQ still shows them as active.

    This proves: cleanup_duplicate_tags deleted them successfully on 2026-03-12,
    but the mirror has not picked up the soft-deletes.
    """
    logger.info('Section A: querying BQ for duplicate taggings still visible in mirror...')

    # Find the entity's full interact_id from the partial prefix
    entity_lookup = _query(bq, f"""
        SELECT interact_id, id AS internal_id
        FROM actionbuilder_cleaned.cln_actionbuilder__entities
        WHERE interact_id LIKE '{entity_id[:8]}%'
        LIMIT 1
    """)
    if not entity_lookup:
        return {
            'error': f'Entity with interact_id prefix {entity_id[:8]} not found in BQ',
            'captured_at': _now_iso(),
        }

    full_entity_id = entity_lookup[0]['interact_id']
    internal_entity_id = entity_lookup[0]['internal_id']
    logger.info(f'  Entity: {full_entity_id} (internal id={internal_entity_id})')

    # Find taggings that are rn > 1 for this entity — these are the ones cleanup deleted.
    # They should show deleted_at IS NOT NULL in the mirror post-deletion, but don't.
    bq_duplicates = _query(bq, f"""
        SELECT
            tl.interact_id          AS tagging_interact_id,
            t.interact_id           AS tag_interact_id,
            t.name                  AS tag_name,
            tl.updated_at,
            tl.deleted_at,
            tl.available
        FROM (
            SELECT
                interact_id,
                tag_id,
                updated_at,
                deleted_at,
                available,
                ROW_NUMBER() OVER (
                    PARTITION BY taggable_id, tag_id, campaign_id
                    ORDER BY updated_at DESC
                ) AS rn
            FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook
            WHERE taggable_id = {internal_entity_id}
              AND deleted_at IS NULL
              AND available = TRUE
              AND taggable_type = 'Entity'
        ) tl
        JOIN actionbuilder_cleaned.cln_actionbuilder__tags t ON t.id = tl.tag_id
        WHERE tl.rn > 1
        ORDER BY t.name, tl.updated_at DESC
    """)

    logger.info(f'  BQ shows {len(bq_duplicates)} excess taggings with deleted_at IS NULL')

    # Also fetch the current live taggings from the AB API
    logger.info('  Fetching current taggings from AB API...')
    try:
        api_taggings = ab.list_person_taggings(campaign_id, full_entity_id)
        api_tagging_ids = {t.get('action_builder:tagging_id') for t in api_taggings
                           if t.get('action_builder:tagging_id')}
    except Exception as e:
        logger.error(f'  Failed to fetch API taggings: {e}')
        api_taggings = []
        api_tagging_ids = set()

    logger.info(f'  AB API returns {len(api_taggings)} active taggings for this entity')

    # Cross-reference: find BQ duplicates absent from API response
    missing_from_api = []
    for row in bq_duplicates:
        tagging_id = str(row['tagging_interact_id'])
        in_api = tagging_id in api_tagging_ids
        missing_from_api.append({
            'tagging_interact_id': tagging_id,
            'tag_interact_id': str(row['tag_interact_id']),
            'tag_name': str(row['tag_name']),
            'bq_updated_at': str(row.get('updated_at', '')),
            'bq_deleted_at': str(row.get('deleted_at', '')),  # should be None
            'bq_available': row.get('available'),
            'present_in_api_response': in_api,
            'evidence': (
                'BQ shows active (deleted_at IS NULL); absent from API list_person_taggings'
                if not in_api else 'Present in both BQ and API (not a discrepancy)'
            ),
        })

    discrepancy_count = sum(1 for r in missing_from_api if not r['present_in_api_response'])

    return {
        'description': (
            'Taggings deleted via AB API (cleanup_duplicate_tags run 2026-03-12) '
            'that still appear active in the BQ mirror with deleted_at IS NULL.'
        ),
        'entity_interact_id': full_entity_id,
        'entity_internal_id': str(internal_entity_id),
        'campaign_interact_id': campaign_id,
        'mirror_stall_date': MIRROR_STALL_DATE,
        'cleanup_run_date': CLEANUP_RUN_DATE,
        'bq_shows_active_count': len(bq_duplicates),
        'api_active_count': len(api_taggings),
        'discrepancy_count': discrepancy_count,
        'discrepancies': missing_from_api,
        'captured_at': _now_iso(),
    }


# ---------------------------------------------------------------------------
# Section B: New writes not appearing in mirror (requires prior sync_log data)
# ---------------------------------------------------------------------------

def capture_section_b(bq: BigQueryConnector) -> Dict[str, Any]:
    """
    Query sync_log for add_tagging operations after the mirror stall date.
    For each logged write, check taggable_logbook for a matching new row.
    Absence = the write succeeded (we logged it) but the mirror didn't pick it up.

    Run this section the day after an update_records run with sync_log active.
    """
    logger.info('Section B: querying sync_log for add_tagging operations...')

    add_tagging_rows = _query(bq, f"""
        SELECT
            sl.entity_interact_id,
            sl.campaign_interact_id,
            sl.executed_at,
            sl.run_id
        FROM actionbuilder_sync.sync_log sl
        WHERE sl.operation = 'add_tagging'
          AND sl.executed_at > '{MIRROR_STALL_DATE}'
        ORDER BY sl.executed_at DESC
        LIMIT 20
    """)

    if not add_tagging_rows:
        return {
            'description': 'No add_tagging entries found in sync_log after mirror stall date.',
            'note': (
                'Run update_records (with sync_log instrumented) first, '
                'then re-run this section the following day.'
            ),
            'mirror_stall_date': MIRROR_STALL_DATE,
            'captured_at': _now_iso(),
        }

    logger.info(f'  Found {len(add_tagging_rows)} add_tagging entries in sync_log')

    # For each entity, check if taggable_logbook has new rows after the write timestamp
    evidence_rows = []
    for row in add_tagging_rows:
        entity_id = row.get('entity_interact_id')
        executed_at = str(row.get('executed_at', ''))

        if not entity_id:
            continue

        # Look up internal entity id
        entity_lookup = _query(bq, f"""
            SELECT id FROM actionbuilder_cleaned.cln_actionbuilder__entities
            WHERE interact_id = '{entity_id}' LIMIT 1
        """)
        if not entity_lookup:
            continue
        internal_id = entity_lookup[0]['id']

        # Check if taggable_logbook has new rows after this write
        new_logbook_rows = _query(bq, f"""
            SELECT COUNT(*) AS cnt
            FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook
            WHERE taggable_id = {internal_id}
              AND created_at > '{executed_at}'
              AND taggable_type = 'Entity'
        """)
        new_count = new_logbook_rows[0]['cnt'] if new_logbook_rows else 0

        evidence_rows.append({
            'entity_interact_id': entity_id,
            'campaign_interact_id': str(row.get('campaign_interact_id', '')),
            'write_executed_at': executed_at,
            'new_logbook_rows_after_write': new_count,
            'evidence': (
                'Write logged in sync_log; no new taggable_logbook rows after write timestamp'
                if new_count == 0 else f'{new_count} new logbook row(s) found after write'
            ),
        })

    missing_count = sum(1 for r in evidence_rows if r['new_logbook_rows_after_write'] == 0)

    return {
        'description': (
            'Tag writes logged in sync_log with no corresponding new rows in taggable_logbook. '
            'Proves writes succeeded (API returned ok) but mirror is not picking them up.'
        ),
        'mirror_stall_date': MIRROR_STALL_DATE,
        'total_add_tagging_logged': len(add_tagging_rows),
        'missing_from_mirror_count': missing_count,
        'evidence': evidence_rows,
        'captured_at': _now_iso(),
    }


# ---------------------------------------------------------------------------
# Section C: Global notes rendering failure
# ---------------------------------------------------------------------------

def capture_section_c(
    ab: ActionBuilderConnector,
    entity_id: str,
    campaign_id: str,
    bq: BigQueryConnector,
) -> Dict[str, Any]:
    """
    Call list_person_taggings for the entity and record any numeric tag values
    present in the API response. The AB UI shows 'Add a number for this response'
    for these same fields, demonstrating an API/UI disagreement entirely within AB.
    """
    logger.info('Section C: fetching taggings from AB API for global notes evidence...')

    # Resolve entity full interact_id if a prefix was provided
    if len(entity_id) < 36:
        entity_lookup = _query(bq, f"""
            SELECT interact_id FROM actionbuilder_cleaned.cln_actionbuilder__entities
            WHERE interact_id LIKE '{entity_id[:8]}%' LIMIT 1
        """)
        if entity_lookup:
            entity_id = entity_lookup[0]['interact_id']
            logger.info(f'  Resolved entity interact_id: {entity_id}')
        else:
            return {
                'error': f'Entity with interact_id prefix {entity_id[:8]} not found in BQ',
                'captured_at': _now_iso(),
            }

    try:
        taggings = ab.list_person_taggings(campaign_id, entity_id)
    except Exception as e:
        return {
            'error': f'Failed to fetch taggings: {e}',
            'entity_interact_id': entity_id,
            'campaign_interact_id': campaign_id,
            'captured_at': _now_iso(),
        }

    logger.info(f'  API returned {len(taggings)} tagging(s)')

    # Extract numeric response fields — these are the ones that fail to render in UI
    numeric_taggings = [
        t for t in taggings
        if t.get('action_builder:number_response') is not None
    ]

    return {
        'description': (
            'Taggings with numeric values present in the AB API response '
            'but not rendered in the AB UI (UI shows "Add a number for this response"). '
            'The API and UI are both part of ActionBuilder — this is an internal AB bug.'
        ),
        'entity_interact_id': entity_id,
        'campaign_interact_id': campaign_id,
        'total_taggings_in_api': len(taggings),
        'numeric_taggings_count': len(numeric_taggings),
        'numeric_taggings': numeric_taggings,
        'full_api_response': taggings,
        'captured_at': _now_iso(),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Capture AB/BQ mirror staleness evidence for bug report.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--sections',
        default='A,C',
        metavar='A,B,C',
        help='Comma-separated sections to run (default: A,C). B requires prior sync_log data.',
    )
    parser.add_argument(
        '--entity',
        default=DEFAULT_ENTITY_ID_PREFIX,
        metavar='ENTITY_ID',
        help='Entity interact_id (or prefix) for Sections A and C. Default: Ciro Amador (CA).',
    )
    parser.add_argument(
        '--campaign',
        default=CALIFORNIA_CAMPAIGN_ID,
        metavar='CAMPAIGN_ID',
        help=f'Campaign interact_id. Default: California ({CALIFORNIA_CAMPAIGN_ID}).',
    )
    args = parser.parse_args()

    sections = [s.strip().upper() for s in args.sections.split(',')]
    invalid = [s for s in sections if s not in ('A', 'B', 'C')]
    if invalid:
        logger.error(f'Unknown section(s): {invalid}. Use A, B, or C.')
        sys.exit(1)

    load_dotenv(dotenv_path='.env')

    logger.info(f'Running sections: {sections}')
    logger.info(f'Entity: {args.entity}, Campaign: {args.campaign}')

    bq = _make_bq_client()
    ab = _make_ab_client()

    report: Dict[str, Any] = {
        'generated_at': _now_iso(),
        'mirror_stall_date': MIRROR_STALL_DATE,
        'sections': {},
    }

    if 'A' in sections:
        logger.info('--- Section A ---')
        report['sections']['A'] = capture_section_a(bq, ab, args.entity, args.campaign)

    if 'B' in sections:
        logger.info('--- Section B ---')
        report['sections']['B'] = capture_section_b(bq)

    if 'C' in sections:
        logger.info('--- Section C ---')
        report['sections']['C'] = capture_section_c(ab, args.entity, args.campaign, bq)

    # Write output
    os.makedirs('evidence', exist_ok=True)
    today = date.today().isoformat()
    out_path = f'evidence/ab_bug_report_{today}.json'

    with open(out_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f'Evidence written to: {out_path}')


if __name__ == '__main__':
    main()
