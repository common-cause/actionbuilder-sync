"""targeted_evidence.py - Targeted AB tag operations for mirror-staleness evidence.

Two modes:

  deletion-check
      Query BQ for an entity's "active" taggings per the stale mirror, then call
      list_person_taggings on the live API. For each tagging_interact_id that BQ
      shows as active (deleted_at IS NULL) but the live API does not return:
      record it as mirror-staleness evidence. Makes NO changes.

  write-check
      Write a small number of tag updates and call list_person_taggings before and
      after each entity write. Diff to capture the new tagging_interact_ids AB
      assigned. Produces evidence of:
        "We wrote tag X to entity Y at time T. AB API assigned tagging_interact_id Z.
         As of write time, Z is not in the BQ mirror. It should appear after recovery."

Both modes output:
    evidence/targeted_{mode}_{YYYY-MM-DD_HH-MM}.json  — full raw data
    evidence/targeted_{mode}_{YYYY-MM-DD_HH-MM}.txt   — human-readable for ticket

Usage:
    # Deletion evidence for Ciro Amador in California (defaults):
    python scripts/targeted_evidence.py deletion-check

    # Deletion evidence for any entity:
    python scripts/targeted_evidence.py deletion-check \\
        --entity <interact_id> --campaign <campaign_id>

    # Write 5 tag updates from updates_needed (all campaigns):
    python scripts/targeted_evidence.py write-check --limit 5

    # Write for a specific campaign only:
    python scripts/targeted_evidence.py write-check --campaign arizona --limit 5

Credentials (in .env):
    BIGQUERY_CREDENTIALS_PASSWORD        Service account JSON
    ACTION_BUILDER_CREDENTIALS_PASSWORD  JSON: {"api_token": "...", "subdomain": "..."}
"""

import argparse
import io
import json
import logging
import os
import sys
import textwrap
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

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

MIRROR_STALL_DATE = '2026-03-05'
CLEANUP_RUN_DATE  = '2026-03-12'

# Ciro Amador — California — known test case for mirror staleness.
# Internal entity id 5420; interact_id resolved from BQ at runtime.
DEFAULT_ENTITY_PREFIX  = 'a78c68e7'
DEFAULT_CAMPAIGN_ID    = 'fd65be58-cce6-400f-97f8-e14adb6558d3'  # California

CAMPAIGN_ALIASES: Dict[str, str] = {
    'arizona':        'a41cde2c-a06f-4fed-8073-b544ca9aead7',
    'california':     'fd65be58-cce6-400f-97f8-e14adb6558d3',
    'colorado':       'c04eece0-5e68-410d-8436-7b28690d4fe0',
    'dc':             '3a227511-fd6f-40f6-abfc-4f2c05ff3b91',
    'florida':        'c998a441-0cc0-405e-a3fe-1b4839ec101a',
    'georgia':        'dd6b11e3-d82a-44c5-91bb-ec516c723fd0',
    'hawaii':         '993e08fe-bdeb-460c-832c-71c1b8c19dba',
    'illinois':       'b6c5d9d8-c382-4da2-85ee-fbf6594d0a04',
    'indiana':        'af5fcde6-2b84-48c3-a8bb-7de045ede252',
    'maryland':       '16702ebe-ddc3-4c80-b832-b9f0a6881f0c',
    'massachusetts':  '51fb121f-a9c6-47a9-a27e-163d0f81b9f2',
    'michigan':       '8407578c-f147-4d50-a91e-245282bc4aa2',
    'minnesota':      'f6b17bf5-90e2-4252-8e7e-cf11ff3f83a0',
    'nebraska':       'e37684a0-1284-49b5-b4aa-855d9faa5ae2',
    'new_mexico':     'feb40677-0ed8-4a1e-9fd2-290526dc6ab1',
    'new_york':       '9f4b8be6-9baf-430d-b548-77227b787f86',
    'north_carolina': '96dca89a-61bd-49f4-87a8-4368e655f1c3',
    'ohio':           '37c5ef62-f4de-4769-ae19-624e5ae42ecd',
    'oregon':         'e8298624-3568-4d92-948b-4429e55d6271',
    'pennsylvania':   'a00b53e0-1ffb-4692-a347-58fe1ad73aa8',
    'rhode_island':   'd5c48860-3764-4020-9d21-ac6024daefa0',
    'test':           '0e41ca37-e05d-499c-943b-9d08dc8725b0',
    'texas':          'c7cf1a2b-a9e5-43dd-93a9-928d4bc979e4',
    'virginia':       '261251df-8836-4f90-a9fb-fdd5dc1798b1',
    'wisconsin':      '12951a1f-6d24-4923-ba31-d4aa6c4c3183',
}

TAG_COLS    = [
    'event_participation_history_tag',
    'event_participation_summary_tag',
    'online_actions_past_6_months_tag',
    'state_online_actions_tag',
    'national_online_actions_tag',
    'engagement_tag',
]
REMOVE_COLS = [c + '_remove' for c in TAG_COLS]


# ---------------------------------------------------------------------------
# Client helpers
# ---------------------------------------------------------------------------

def _make_bq() -> BigQueryConnector:
    bq = BigQueryConnector(project_id=BQ_PROJECT)
    bq.connect()
    return bq


def _make_ab() -> ActionBuilderConnector:
    ab = ActionBuilderConnector()
    ab.connect()
    return ab


def _query(bq: BigQueryConnector, sql: str) -> List[Dict[str, Any]]:
    return [dict(r) for r in bq.query(sql)]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_campaign(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    key = value.lower().replace(' ', '_').replace('-', '_')
    return CAMPAIGN_ALIASES.get(key, value)


def _get_campaign_map(bq: BigQueryConnector) -> Dict[str, str]:
    """int-id → interact_id for all active campaigns."""
    rows = _query(bq, """
        SELECT CAST(id AS STRING) AS int_id, interact_id
        FROM actionbuilder_cleaned.cln_actionbuilder__campaigns
        WHERE status = 'active'
    """)
    m: Dict[str, str] = {}
    for r in rows:
        m[r['int_id']] = r['interact_id']
        m[r['interact_id']] = r['interact_id']
    return m


# ---------------------------------------------------------------------------
# Tagging helpers
# ---------------------------------------------------------------------------

def _tagging_id(t: Dict[str, Any]) -> Optional[str]:
    """
    Extract tagging interact_id from a raw tagging object.
    Tries multiple locations since different AB endpoints vary.
    """
    # Direct field variants
    for key in ('interact_id', 'action_builder:interact_id',
                'action_builder:tagging_interact_id', 'tagging_id'):
        v = t.get(key)
        if v:
            return str(v)
    # identifiers list: ["action_builder:{uuid}", ...]
    for ident in (t.get('identifiers') or []):
        if isinstance(ident, str) and ident.startswith('action_builder:'):
            candidate = ident[len('action_builder:'):]
            if len(candidate) == 36 and candidate.count('-') == 4:
                return candidate
    # _links.self.href last path segment
    href = (t.get('_links') or {}).get('self', {}).get('href', '')
    if href:
        seg = href.rstrip('/').split('/')[-1]
        if len(seg) == 36 and seg.count('-') == 4:
            return seg
    return None


def _tagging_summary(t: Dict[str, Any]) -> Dict[str, Any]:
    """Compact representation of one tagging for evidence output."""
    return {
        'tagging_interact_id': _tagging_id(t),
        'section': t.get('action_builder:section'),
        'field':   t.get('action_builder:field'),
        'tag_name': t.get('action_builder:name') or t.get('name'),
        'response': (
            t.get('action_builder:response')
            or t.get('action_builder:number_response')
            or t.get('action_builder:date_response')
        ),
    }


def _fetch_taggings(ab: ActionBuilderConnector,
                    campaign_id: str,
                    entity_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Return {tagging_interact_id: summary_dict} for all live taggings on an entity.
    """
    raw = ab.list_person_taggings(campaign_id, entity_id)
    result = {}
    for t in raw:
        tid = _tagging_id(t)
        if tid:
            result[tid] = _tagging_summary(t)
        else:
            # Keep with a placeholder key so nothing is silently dropped
            result[f'unknown_{len(result)}'] = _tagging_summary(t)
    return result


def _diff_taggings(
    before: Dict[str, Any],
    after:  Dict[str, Any],
) -> Tuple[List[Dict], List[Dict]]:
    """
    Return (added, removed) lists comparing before→after tagging sets.
    Each element is a summary dict with the tagging_interact_id included.
    """
    before_ids: Set[str] = set(before.keys())
    after_ids:  Set[str] = set(after.keys())
    added   = [after[tid]  for tid in after_ids  - before_ids]
    removed = [before[tid] for tid in before_ids - after_ids]
    return added, removed


def _parse_sync_string(s: str) -> Dict[str, Any]:
    """
    Parse an AB sync string into a tag dict for the Person Signup Helper add_tags.

    Format:  "Section:|:Category:|:FieldName:|:response_type:value"

    The AB tagging resource structure is:
        action_builder:section  → parts[0]  (e.g. "Participation")
        action_builder:field    → parts[1]  (category, e.g. "Event Attendance Summary")
        action_builder:name     → parts[2]  (field/data-point name, e.g. "Phone Bank Calls Made")
        action_builder:number_response / date_response → parsed value

    The signup helper 'name' key maps to action_builder:name in the tagging resource.
    """
    parts = s.split(':|:')
    if len(parts) != 4:
        raise ValueError(f'Bad sync string: {s!r}')
    response_type, value = parts[3].split(':', 1)
    tag: Dict[str, Any] = {
        'action_builder:section': parts[0],
        'action_builder:field':   parts[1],   # category
        'name':                   parts[2],   # field/data-point name
    }
    if response_type == 'number_response':
        try:
            tag['action_builder:number_response'] = int(value)
        except ValueError:
            tag['action_builder:number_response'] = float(value)
    elif response_type == 'date_response':
        tag['action_builder:date_response'] = value
    # standard_response: 'name' already identifies the option; no extra key needed
    return tag


def _parse_removal_string(s: str) -> Tuple[str, str]:
    parts = s.split(':|:')
    if len(parts) != 2:
        raise ValueError(f'Bad removal string: {s!r}')
    return parts[0], parts[1]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _save(mode: str, data: Dict[str, Any], summary_lines: List[str]) -> None:
    os.makedirs('evidence', exist_ok=True)
    stamp = _now().strftime('%Y-%m-%d_%H-%M')
    base  = f'evidence/targeted_{mode}_{stamp}'

    json_path = base + '.json'
    txt_path  = base + '.txt'

    with open(json_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)

    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary_lines) + '\n')

    logger.info(f'Output written:')
    logger.info(f'  {json_path}')
    logger.info(f'  {txt_path}')


# ---------------------------------------------------------------------------
# Mode: deletion-check
# ---------------------------------------------------------------------------

def deletion_check(
    bq: BigQueryConnector,
    ab: ActionBuilderConnector,
    entity_arg:   str,
    campaign_arg: str,
) -> None:
    """
    Compare BQ mirror taggings against live API for one entity.
    Records every tagging_interact_id that BQ shows as active but the API omits.
    """
    logger.info('deletion-check: resolving entity interact_id from BQ...')

    entity_rows = _query(bq, f"""
        SELECT interact_id, id AS internal_id,
               first_name, last_name
        FROM actionbuilder_cleaned.cln_actionbuilder__entities
        WHERE interact_id LIKE '{entity_arg[:8]}%'
        LIMIT 1
    """)
    if not entity_rows:
        logger.error(f'Entity with prefix {entity_arg[:8]} not found')
        sys.exit(1)

    entity_id   = entity_rows[0]['interact_id']
    internal_id = entity_rows[0]['internal_id']
    given       = entity_rows[0].get('first_name') or ''
    family      = entity_rows[0].get('last_name') or ''
    name        = f'{given} {family}'.strip()

    logger.info(f'  Entity: {name!r} — {entity_id} (internal {internal_id})')
    logger.info(f'  Campaign: {campaign_arg}')

    # -- BQ: find all taggings BQ thinks are active (deleted_at IS NULL) ----------
    logger.info('deletion-check: querying BQ for all active taggings...')
    bq_all = _query(bq, f"""
        SELECT
            tl.interact_id   AS tagging_interact_id,
            t.interact_id    AS tag_interact_id,
            t.name           AS tag_name,
            tl.updated_at,
            tl.created_at,
            ROW_NUMBER() OVER (
                PARTITION BY tl.taggable_id, tl.tag_id, tl.campaign_id
                ORDER BY tl.updated_at DESC
            ) AS rn
        FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook tl
        JOIN actionbuilder_cleaned.cln_actionbuilder__tags t ON t.id = tl.tag_id
        WHERE tl.taggable_id = {internal_id}
          AND tl.deleted_at IS NULL
          AND tl.available = TRUE
          AND tl.taggable_type = 'Entity'
        ORDER BY t.name, tl.updated_at DESC
    """)
    logger.info(f'  BQ shows {len(bq_all)} active tagging(s) (all rn)')

    bq_keepers   = {r['tagging_interact_id']: r for r in bq_all if r['rn'] == 1}
    bq_excess    = {r['tagging_interact_id']: r for r in bq_all if r['rn'] >  1}

    logger.info(f'  BQ keepers (rn=1): {len(bq_keepers)} | excess (rn>1): {len(bq_excess)}')

    # -- Live API ----------------------------------------------------------------
    logger.info('deletion-check: calling list_person_taggings on live API...')
    captured_at = _now()
    api_taggings = _fetch_taggings(ab, campaign_arg, entity_id)
    api_ids: Set[str] = set(api_taggings.keys())
    logger.info(f'  API returns {len(api_ids)} active tagging(s)')

    # -- Cross-reference ---------------------------------------------------------
    # Excess taggings (cleanup targets): present in BQ, absent from API
    excess_absent = {tid: r for tid, r in bq_excess.items() if tid not in api_ids}
    excess_still  = {tid: r for tid, r in bq_excess.items() if tid in api_ids}

    # Keepers: should be present in both
    keepers_absent = {tid: r for tid, r in bq_keepers.items() if tid not in api_ids}
    keepers_present = {tid: r for tid, r in bq_keepers.items() if tid in api_ids}

    # In API but not in BQ at all (not even in bq_all)
    bq_all_ids = {r['tagging_interact_id'] for r in bq_all}
    api_only = {tid: api_taggings[tid] for tid in api_ids if tid not in bq_all_ids}

    # -- Build output ------------------------------------------------------------
    data = {
        'mode': 'deletion-check',
        'captured_at': captured_at.isoformat(),
        'mirror_stall_date': MIRROR_STALL_DATE,
        'cleanup_run_date': CLEANUP_RUN_DATE,
        'entity': {
            'interact_id': entity_id,
            'internal_id': str(internal_id),
            'name': name,
            'campaign_interact_id': campaign_arg,
        },
        'counts': {
            'bq_active_total':   len(bq_all),
            'bq_keepers':        len(bq_keepers),
            'bq_excess':         len(bq_excess),
            'api_active':        len(api_ids),
            'excess_absent_from_api':  len(excess_absent),
            'excess_still_in_api':     len(excess_still),
            'keepers_absent_from_api': len(keepers_absent),
            'keepers_present_in_api':  len(keepers_present),
            'in_api_not_in_bq':        len(api_only),
        },
        'excess_absent_from_api': [
            {
                'tagging_interact_id': tid,
                'tag_name': r['tag_name'],
                'bq_updated_at': str(r.get('updated_at', '')),
                'bq_created_at': str(r.get('created_at', '')),
                'in_bq_mirror': True,
                'in_live_api':  False,
                'interpretation': 'Deleted by cleanup_duplicate_tags 2026-03-12; BQ mirror not updated',
            }
            for tid, r in sorted(excess_absent.items(),
                                  key=lambda x: x[1].get('tag_name', ''))
        ],
        'excess_still_in_api': [
            {'tagging_interact_id': tid, 'tag_name': r['tag_name']}
            for tid, r in excess_still.items()
        ],
        'keepers_absent_from_api': [
            {'tagging_interact_id': tid, 'tag_name': r['tag_name']}
            for tid, r in keepers_absent.items()
        ],
        'in_api_not_in_bq': list(api_only.values()),
        'api_full_response': list(api_taggings.values()),
    }

    # -- Human-readable summary --------------------------------------------------
    lines = [
        '=' * 70,
        'AB MIRROR STALENESS EVIDENCE — DELETION CHECK',
        f'Generated: {captured_at.strftime("%Y-%m-%d %H:%M UTC")}',
        f'Mirror known stalled since: {MIRROR_STALL_DATE}',
        f'Cleanup run (duplicate tag deletion): {CLEANUP_RUN_DATE}',
        '=' * 70,
        '',
        f'Entity:   {name} ({entity_id})',
        f'Campaign: {campaign_arg}',
        '',
        'SUMMARY',
        '-------',
        f'BQ mirror shows {len(bq_all)} active tagging(s) total '
        f'({len(bq_keepers)} unique tag/entity pairs + {len(bq_excess)} excess duplicates)',
        f'Live AB API returns {len(api_ids)} active tagging(s)',
        '',
        f'DISCREPANCY: {len(excess_absent)} tagging(s) BQ shows active but API has already deleted',
        f'  (These were removed by cleanup_duplicate_tags on {CLEANUP_RUN_DATE})',
        f'  (AB UI confirmed clean post-cleanup; mirror has not caught up)',
        '',
    ]

    if excess_absent:
        lines.append('TAGGINGS BQ SHOWS AS ACTIVE — ABSENT FROM LIVE API:')
        lines.append(f'  {"tagging_interact_id":<38}  {"tag_name"}')
        lines.append(f'  {"-"*38}  {"-"*30}')
        for item in data['excess_absent_from_api']:
            lines.append(
                f'  {item["tagging_interact_id"]:<38}  {item["tag_name"]}'
                f'  (BQ updated_at: {item["bq_updated_at"][:10]})'
            )
        lines.append('')
        lines.append(
            'Each tagging_interact_id above should have deleted_at set in '
            'taggable_logbook as of 2026-03-12. The mirror shows deleted_at IS NULL.'
        )
    else:
        lines.append('No excess taggings found in BQ for this entity.')

    if excess_still:
        lines.append('')
        lines.append(
            f'NOTE: {len(excess_still)} excess tagging(s) still present in live API '
            '(not yet cleaned by most recent run):'
        )
        for item in data['excess_still_in_api']:
            lines.append(f'  {item["tagging_interact_id"]}  {item["tag_name"]}')

    if keepers_absent:
        lines.append('')
        lines.append(
            f'ADDITIONAL ISSUE: {len(keepers_absent)} keeper tagging(s) BQ shows '
            'as active are also absent from the live API:'
        )
        for item in data['keepers_absent_from_api']:
            lines.append(f'  {item["tagging_interact_id"]}  {item["tag_name"]}')

    if api_only:
        lines.append('')
        lines.append(
            f'NOTE: {len(api_only)} tagging(s) present in live API but not in BQ at all '
            '(new writes not yet replicated to mirror):'
        )
        for item in data['in_api_not_in_bq']:
            lines.append(
                f'  tagging_id={item.get("tagging_interact_id")}  '
                f'field={item.get("field")}  value={item.get("response")}'
            )

    _save('deletion-check', data, lines)

    # Print summary to console (safe for Windows cp1252)
    out = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    out.write('\n')
    for line in lines:
        out.write(line + '\n')
    out.flush()


# ---------------------------------------------------------------------------
# Mode: write-check
# ---------------------------------------------------------------------------

def write_check(
    bq: BigQueryConnector,
    ab: ActionBuilderConnector,
    campaign_filter: Optional[str],
    limit: int,
    entity_filter: Optional[str] = None,
) -> None:
    """
    Write a small batch of tag updates from updates_needed and capture
    list_person_taggings before + after each entity to document new
    tagging_interact_ids. Produces evidence of writes not (yet) in BQ mirror.
    """
    logger.info('write-check: fetching campaign map...')
    campaign_map = _get_campaign_map(bq)

    logger.info('write-check: fetching rows from updates_needed...')
    where_parts = []
    if campaign_filter:
        campaign_int_ids = [k for k, v in campaign_map.items() if v == campaign_filter and k != campaign_filter]
        if campaign_int_ids:
            where_parts.append(f'campaign_id = {campaign_int_ids[0]}')
    if entity_filter:
        where_parts.append(f"entity_id = '{entity_filter}'")
    where_clause = ('WHERE ' + ' AND '.join(where_parts)) if where_parts else ''
    fetch_limit  = limit * 20 if (campaign_filter or entity_filter) else limit * 10
    sql = (
        f'SELECT * FROM `{BQ_PROJECT}.actionbuilder_sync.updates_needed` '
        f'{where_clause} LIMIT {fetch_limit}'
    )
    rows = _query(bq, sql)

    if not rows:
        logger.info('write-check: updates_needed is empty — nothing to write')
        return

    # Group rows by (campaign_interact_id, entity_id), honouring campaign filter
    groups: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(
        lambda: {'add_tags': [], 'removals': [], 'entity_id': '', 'campaign_id': ''}
    )
    for row in rows:
        campaign_id = campaign_map.get(str(row['campaign_id']), str(row['campaign_id']))
        entity_id   = str(row['entity_id'])

        if campaign_filter and campaign_id != campaign_filter:
            continue

        key = (campaign_id, entity_id)
        groups[key]['entity_id']   = entity_id
        groups[key]['campaign_id'] = campaign_id

        for col in TAG_COLS:
            val = row.get(col)
            if val:
                try:
                    tag = _parse_sync_string(str(val))
                    # Carry the column name so we can label it in output
                    tag['_source_col'] = col
                    groups[key]['add_tags'].append(tag)
                except ValueError:
                    pass

        for col in REMOVE_COLS:
            val = row.get(col)
            if val:
                try:
                    groups[key]['removals'].append(_parse_removal_string(str(val)))
                except ValueError:
                    pass

    # Take the first `limit` groups that have something to add
    writable = [
        (k, v) for k, v in groups.items()
        if v['add_tags']
    ][:limit]

    if not writable:
        logger.info('write-check: no groups with add_tags found for this filter')
        return

    logger.info(f'write-check: will process {len(writable)} entity/campaign group(s)')

    results = []
    captured_at = _now()

    for (campaign_id, entity_id), data in writable:
        add_tags  = data['add_tags']
        removals  = data['removals']
        label     = f'entity={entity_id[:8]}... campaign={campaign_id[:8]}...'

        logger.info(f'  Processing {label}')

        # Fetch entity name for readability
        entity_meta = _query(bq, f"""
            SELECT first_name, last_name
            FROM actionbuilder_cleaned.cln_actionbuilder__entities
            WHERE interact_id = '{entity_id}' LIMIT 1
        """)
        name = ''
        if entity_meta:
            name = f"{entity_meta[0].get('first_name','')} {entity_meta[0].get('last_name','')}".strip()

        # Resolve campaign name
        campaign_name = next(
            (k for k, v in CAMPAIGN_ALIASES.items() if v == campaign_id), campaign_id[:8]
        )

        # Before state
        logger.info(f'    Fetching before-state taggings...')
        before = _fetch_taggings(ab, campaign_id, entity_id)

        # Delete old taggings
        deletions_done = []
        for tag_id, tagging_id in removals:
            logger.info(f'    Deleting tagging {tagging_id[:8]}...')
            status = ab.delete_tagging(campaign_id, tag_id, tagging_id)
            deletions_done.append({
                'tagging_interact_id': tagging_id,
                'tag_interact_id':     tag_id,
                'status':              status,
            })

        # Write new tag values
        write_time = _now()
        logger.info(f'    Writing {len(add_tags)} tag(s)...')
        # Strip internal _source_col key before sending to API
        api_add_tags = [
            {k: v for k, v in t.items() if not k.startswith('_')}
            for t in add_tags
        ]
        api_response = ab.update_entity_with_tags(campaign_id, entity_id, api_add_tags)

        # Brief pause — AB sometimes takes a moment to commit a write before
        # it's visible via list_person_taggings.
        import time as _time
        _time.sleep(3)

        # After state
        logger.info(f'    Fetching after-state taggings...')
        after_raw = ab.list_person_taggings(campaign_id, entity_id)
        after = {}
        for t in after_raw:
            tid = _tagging_id(t)
            if tid:
                after[tid] = _tagging_summary(t)
            else:
                after[f'unknown_{len(after)}'] = _tagging_summary(t)

        added, removed = _diff_taggings(before, after)

        # Check whether new tagging IDs appear in BQ mirror
        new_ids = [t['tagging_interact_id'] for t in added if t.get('tagging_interact_id')]
        bq_present = []
        if new_ids:
            id_list = ', '.join(f"'{tid}'" for tid in new_ids if tid)
            bq_check = _query(bq, f"""
                SELECT interact_id, deleted_at
                FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook
                WHERE interact_id IN ({id_list})
            """)
            bq_present = [r['interact_id'] for r in bq_check]

        result = {
            'entity_interact_id':  entity_id,
            'entity_name':         name,
            'campaign_interact_id': campaign_id,
            'campaign_name':        campaign_name,
            'write_executed_at':    write_time.isoformat(),
            'tags_written': [
                {
                    'section':    t.get('action_builder:section'),
                    'field':      t.get('action_builder:field'),
                    'value':      t.get('name'),
                    'source_col': t.get('_source_col'),
                }
                for t in add_tags
            ],
            'deletions_performed': deletions_done,
            'before_tagging_count': len(before),
            'after_tagging_count':  len(after),
            'new_taggings': added,
            'removed_taggings': removed,
            'new_tagging_interact_ids_in_bq_mirror': bq_present,
            'api_response_person_resource': api_response,
            'raw_after_tagging_sample': after_raw[:2] if after_raw else [],
            'mirror_staleness_evidence': [
                {
                    'tagging_interact_id': t['tagging_interact_id'],
                    'field':     t.get('field'),
                    'value':     t.get('response'),
                    'written_at': write_time.isoformat(),
                    'in_bq_mirror_at_write_time': t['tagging_interact_id'] in bq_present
                    if t.get('tagging_interact_id') else None,
                }
                for t in added
            ],
        }
        results.append(result)

        logger.info(
            f'    Done: {len(added)} new tagging(s), '
            f'{len(removed)} removed, '
            f'{sum(1 for t in added if t.get("tagging_interact_id") not in bq_present)} '
            f'new ID(s) not yet in BQ mirror'
        )

    full_data = {
        'mode': 'write-check',
        'captured_at': captured_at.isoformat(),
        'mirror_stall_date': MIRROR_STALL_DATE,
        'results': results,
    }

    # -- Human-readable summary --------------------------------------------------
    lines = [
        '=' * 70,
        'AB MIRROR STALENESS EVIDENCE — WRITE CHECK',
        f'Generated: {captured_at.strftime("%Y-%m-%d %H:%M UTC")}',
        f'Mirror known stalled since: {MIRROR_STALL_DATE}',
        '=' * 70,
        '',
        f'Processed {len(results)} entity/campaign group(s).',
        '',
        'For each write below: the tagging_interact_id was returned by the live',
        'AB API (via list_person_taggings after write). It should appear in',
        'taggable_logbook.interact_id once the mirror catches up.',
        '',
    ]

    total_new = 0
    total_missing = 0

    for r in results:
        lines.append('-' * 70)
        lines.append(
            f'Entity:   {r["entity_name"]} ({r["entity_interact_id"]})'
        )
        lines.append(f'Campaign: {r["campaign_name"]} ({r["campaign_interact_id"]})')
        lines.append(f'Written:  {r["write_executed_at"]}')
        lines.append('')

        if r['tags_written']:
            lines.append('  Tags written:')
            for t in r['tags_written']:
                lines.append(f'    [{t["section"]}] {t["field"]}: {t["value"]!r}')
            lines.append('')

        if r['new_taggings']:
            lines.append('  New tagging_interact_ids assigned by AB API:')
            for t in r['new_taggings']:
                tid = t.get('tagging_interact_id', '(unknown)')
                in_bq = tid in r['new_tagging_interact_ids_in_bq_mirror']
                lines.append(
                    f'    {tid}  field={t.get("field")}  value={t.get("response")}'
                    f'  [BQ mirror: {"YES" if in_bq else "NOT YET"}]'
                )
            total_new += len(r['new_taggings'])
            total_missing += sum(
                1 for t in r['new_taggings']
                if t.get('tagging_interact_id') not in r['new_tagging_interact_ids_in_bq_mirror']
            )
        else:
            lines.append(
                '  (No new tagging IDs detected in before/after diff — '
                'tagging_id extraction may need adjustment for this API version)'
            )

        if r['deletions_performed']:
            lines.append('')
            lines.append(f'  Deletions ({len(r["deletions_performed"])}):')
            for d in r['deletions_performed']:
                lines.append(f'    {d["tagging_interact_id"]}  status={d["status"]}')

        lines.append('')

    lines += [
        '=' * 70,
        f'TOTAL: {total_new} new tagging(s) written across {len(results)} entity/campaign group(s)',
        f'       {total_missing} new tagging_interact_id(s) confirmed absent from BQ mirror at write time',
        '',
        'ACTION REQUIRED FROM ACTIONBUILDER:',
        '  The tagging_interact_ids listed above exist in the live AB production',
        '  database (confirmed via list_person_taggings API response). They should',
        '  appear in the taggable_logbook mirror with created_at ≈ write time above.',
        '  As of this report, they do not. Please investigate mirror replication.',
        '=' * 70,
    ]

    _save('write-check', full_data, lines)

    out = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    out.write('\n')
    for line in lines:
        out.write(line + '\n')
    out.flush()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Targeted AB tag operations for mirror-staleness evidence.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='mode', required=True)

    # deletion-check
    dc = sub.add_parser(
        'deletion-check',
        help='Compare BQ mirror vs live API for one entity (no changes made)',
    )
    dc.add_argument(
        '--entity', default=DEFAULT_ENTITY_PREFIX,
        help='Entity interact_id or first-8-chars prefix (default: Ciro Amador CA)',
    )
    dc.add_argument(
        '--campaign', default=DEFAULT_CAMPAIGN_ID,
        help='Campaign interact_id or alias (default: California)',
    )

    # write-check
    wc = sub.add_parser(
        'write-check',
        help='Write a small batch of tag updates and capture new tagging interact_ids',
    )
    wc.add_argument(
        '--campaign', default=None,
        help='Optional campaign filter (alias or UUID). Omit to use all campaigns.',
    )
    wc.add_argument(
        '--entity', default=None,
        metavar='ENTITY_ID',
        help='Optional entity interact_id to target a single entity.',
    )
    wc.add_argument(
        '--limit', type=int, default=5,
        help='Number of entity/campaign groups to process (default: 5)',
    )

    args = parser.parse_args()
    load_dotenv(dotenv_path='.env')

    bq = _make_bq()
    ab = _make_ab()

    if args.mode == 'deletion-check':
        campaign_id = _resolve_campaign(args.campaign) or args.campaign
        deletion_check(bq, ab, args.entity, campaign_id)

    elif args.mode == 'write-check':
        campaign_filter = _resolve_campaign(args.campaign)
        write_check(bq, ab, campaign_filter, args.limit, entity_filter=args.entity)


if __name__ == '__main__':
    main()
