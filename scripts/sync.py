"""sync.py - ActionBuilder sync script.

Reads data from BigQuery (via dbt models) and pushes updates to ActionBuilder.

Operations:
    update_records      Update tags on existing AB entities (reads updates_needed)
    insert_new_records  Insert genuinely new entities into AB (reads deduplicated_names_to_load)
    remove_records      Delete duplicate entities (reads dedup_candidates)
    prepare_email_data  Add secondary emails to keeper entities (reads email_migration_needed)
    prepare_phone_data  Add secondary phones to keeper entities (reads phone_migration_needed)
    apply_assessments   Write auto-assessment levels to entities (reads auto_assessment_rules)

Usage:
    python scripts/sync.py <operation> [--campaign CAMPAIGN_ID] [--dry-run] [--limit N]

    --campaign   Optional filter: only process rows for this campaign interact_id UUID.
                 When omitted, all rows are processed using per-row campaign_interact_id
                 from BQ. Useful for running one state at a time (e.g. --campaign arizona).
    --dry-run    Fetch data and log what WOULD happen; no API writes.
    --limit N    Process only first N rows (useful for test campaign validation).

Credentials (in .env):
    BIGQUERY_CREDENTIALS_PASSWORD   Service account JSON (already present)
    ACTION_BUILDER_CREDENTIALS_PASSWORD JSON: {"api_token": "...", "subdomain": "..."}

Execution order (before first production run):
    1. prepare_email_data   (migrate secondary emails to keeper entities)
    2. prepare_phone_data   (migrate secondary phones to keeper entities)
    3. remove_records       (delete duplicate entities)
    4. Resolve open dedup_unresolved pairs (unblocks held-out new records)
    5. insert_new_records   (create genuinely new entities)
    6. update_records       (keep participation data current -- recurring)

Notes:
    - Campaign IDs come from BQ data (campaign_interact_id column in each view).
      Every write operation uses the per-row campaign to ensure the entity is
      accessible in that campaign. --campaign is only a filter, never the source
      of the campaign ID used for API calls.
    - updates_needed has one row per (campaign_id, entity_id, field_name). The
      campaign_id column is an internal integer; this script looks up the
      interact_id UUID via a supplementary BQ query before making API calls.
    - The *_field columns in deduplicated_names_to_load are plain field-name
      strings, not sync strings. This script builds add_tags from the numeric /
      date value columns (action_network_actions, events_6m, etc.) using the
      INSERT_TAG_FIELDS mapping defined below.
"""

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

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
# BigQuery dataset / project
# ---------------------------------------------------------------------------
BQ_PROJECT = 'proj-tmc-mem-com'
BQ_DATASET = 'actionbuilder_sync'

# ---------------------------------------------------------------------------
# Tag columns in updates_needed
# ---------------------------------------------------------------------------
TAG_COLS = [
    'event_participation_history_tag',
    'event_participation_summary_tag',
    'online_actions_past_6_months_tag',
    'state_online_actions_tag',
    'national_online_actions_tag',
    'engagement_tag',
]
REMOVE_COLS = [c + '_remove' for c in TAG_COLS]

# ---------------------------------------------------------------------------
# Tag field mapping for insert_new_records
#
# Maps value column name in deduplicated_names_to_load
# -> (section, field, response_type)
#
# The *_field columns in that view are plain strings like "Action Network
# Actions", not sync strings. We build add_tags directly from the numeric /
# date value columns using this mapping.
# ---------------------------------------------------------------------------
INSERT_TAG_FIELDS: Dict[str, Tuple[str, str, str]] = {
    'action_network_actions': (
        'Participation', 'Action Network Actions', 'number_response',
    ),
    'events_6m': (
        'Participation', 'Events Attended Past 6 Months', 'number_response',
    ),
    'phone_bank_dials': (
        'Participation', 'Phone Bank Calls Made', 'number_response',
    ),
    'first_event_date': (
        'Participation', 'First Event Attended', 'date_response',
    ),
    'mr_event_date': (
        'Participation', 'Most Recent Event Attended', 'date_response',
    ),
}

# ---------------------------------------------------------------------------
# Known campaign name aliases
# Allows --campaign arizona in addition to the full UUID.
# ---------------------------------------------------------------------------
CAMPAIGN_ALIASES: Dict[str, str] = {
    'arizona':        'a41cde2c-a06f-4fed-8073-b544ca9aead7',
    'california':     'fd65be58-cce6-400f-97f8-e14adb6558d3',
    'colorado':       'c04eece0-5e68-410d-8436-7b28690d4fe0',
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
    'wisconsin':      '12951a1f-6d24-4923-ba31-d4aa6c4c3183',
}


def _resolve_campaign_arg(value: Optional[str]) -> Optional[str]:
    """
    Resolve a --campaign value to a full UUID.

    Accepts:
      - Full UUID (returned as-is)
      - Lowercase state name alias (e.g. "arizona", "north_carolina")
    Returns None if value is None.
    """
    if value is None:
        return None
    normalized = value.lower().replace(' ', '_').replace('-', '_')
    if normalized in CAMPAIGN_ALIASES:
        return CAMPAIGN_ALIASES[normalized]
    return value


# ---------------------------------------------------------------------------
# Client helpers
# ---------------------------------------------------------------------------

def _make_bq_client() -> BigQueryConnector:
    """Build a BigQuery client using ccef-connections BigQueryConnector."""
    bq = BigQueryConnector(project_id=BQ_PROJECT)
    bq.connect()
    return bq


def _make_ab_client() -> ActionBuilderConnector:
    """Build a connected ActionBuilderConnector (reads ACTION_BUILDER_CREDENTIALS_PASSWORD)."""
    ab = ActionBuilderConnector()
    ab.connect()
    return ab


# ---------------------------------------------------------------------------
# BQ query helpers
# ---------------------------------------------------------------------------

def _query(bq: BigQueryConnector, sql: str) -> List[Dict[str, Any]]:
    """Run a BigQuery query and return rows as list of dicts."""
    rows = list(bq.query(sql))
    return [dict(r) for r in rows]


def _get_campaign_map(bq: BigQueryConnector) -> Dict[str, str]:
    """
    Return a dict mapping both:
      - campaign integer id (as string) -> interact_id UUID
      - campaign interact_id UUID        -> interact_id UUID  (passthrough)

    Allows update_records to resolve the campaign_id column in updates_needed
    regardless of whether it is stored as an integer or a UUID.
    """
    rows = _query(bq, """
        SELECT CAST(id AS STRING) AS int_id, interact_id
        FROM actionbuilder_cleaned.cln_actionbuilder__campaigns
        WHERE status = 'active'
    """)
    mapping: Dict[str, str] = {}
    for r in rows:
        mapping[r['int_id']] = r['interact_id']
        mapping[r['interact_id']] = r['interact_id']
    return mapping


# ---------------------------------------------------------------------------
# Sync-string / removal-string parsers
# ---------------------------------------------------------------------------

def parse_sync_string(s: str) -> Dict[str, str]:
    """
    Parse an AB sync string into a tag dict suitable for add_tags.

    Format:  "Section:|:Category:|:Field:|:response_type:value"
    Returns: {"action_builder:section": ..., "action_builder:field": ..., "name": ...}
    """
    parts = s.split(':|:')
    if len(parts) != 4:
        raise ValueError(f'Expected 4 parts in sync string, got {len(parts)}: {s!r}')
    section = parts[0]
    field = parts[2]
    value = parts[3].split(':', 1)[1]   # strip "number_response:" prefix
    return {
        'action_builder:section': section,
        'action_builder:field': field,
        'name': value,
    }


def parse_removal_string(s: str) -> Tuple[str, str]:
    """
    Parse an AB removal string into (tag_interact_id, tagging_interact_id).

    Format: "tag-uuid:|:tagging-uuid"
    """
    parts = s.split(':|:')
    if len(parts) != 2:
        raise ValueError(
            f'Expected 2 parts in removal string, got {len(parts)}: {s!r}'
        )
    return parts[0], parts[1]


def _build_insert_tag(col: str, value: Any) -> Optional[Dict[str, str]]:
    """
    Build a tag dict for an insert_new_records field from a column value.

    Returns None if the value is falsy (0, None, empty string).
    """
    if not value:
        return None
    mapping = INSERT_TAG_FIELDS.get(col)
    if not mapping:
        return None
    section, field, _ = mapping
    str_value = str(value) if not isinstance(value, date) else str(value)
    return {
        'action_builder:section': section,
        'action_builder:field': field,
        'name': str_value,
    }


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------

def update_records(
    bq: BigQueryConnector,
    ab: Optional[ActionBuilderConnector],
    campaign_filter: Optional[str],
    dry_run: bool,
    limit: Optional[int],
) -> None:
    """
    Update tags on existing ActionBuilder entities.

    Reads actionbuilder_sync.updates_needed (one row per campaign/entity/field
    needing an update). Groups rows by (campaign_interact_id, entity_id) and
    for each group:
      a. Deletes existing taggings listed in *_tag_remove columns.
      b. Posts new tag values from *_tag columns via update_entity_with_tags.
    """
    logger.info('update_records: fetching campaign ID map...')
    campaign_map = _get_campaign_map(bq)

    logger.info('update_records: fetching rows from updates_needed...')
    sql = f'SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.updates_needed`'
    if limit:
        sql += f' LIMIT {limit}'
    rows = _query(bq, sql)

    if not rows:
        logger.info('update_records: no rows to process')
        return

    # Group by (campaign_interact_id, entity_id)
    # Each group accumulates: add_tags list, removals list of (tag_id, tagging_id)
    groups: Dict[
        Tuple[str, str], Dict[str, Any]
    ] = defaultdict(lambda: {'add_tags': [], 'removals': []})

    for row in rows:
        campaign_id_raw = str(row['campaign_id'])
        entity_id = str(row['entity_id'])
        campaign_interact_id = campaign_map.get(campaign_id_raw, campaign_id_raw)

        if campaign_filter and campaign_interact_id != campaign_filter:
            continue

        key = (campaign_interact_id, entity_id)

        for col in TAG_COLS:
            val = row.get(col)
            if val:
                try:
                    groups[key]['add_tags'].append(parse_sync_string(str(val)))
                except ValueError as e:
                    logger.warning(f'  Skipping malformed sync string in {col}: {e}')

        for col in REMOVE_COLS:
            val = row.get(col)
            if val:
                try:
                    groups[key]['removals'].append(parse_removal_string(str(val)))
                except ValueError as e:
                    logger.warning(f'  Skipping malformed removal string in {col}: {e}')

    logger.info(
        f'update_records: {len(groups)} (campaign, entity) groups to process'
    )

    n_ok = n_err = n_tags_added = n_removals_done = 0

    for (campaign_id, entity_id), data in groups.items():
        add_tags: List[Dict[str, str]] = data['add_tags']
        removals: List[Tuple[str, str]] = data['removals']
        label = f'entity={entity_id[:8]}... campaign={campaign_id[:8]}...'

        if dry_run:
            logger.info(
                f'  [DRY-RUN] Would delete {len(removals)} tagging(s) and '
                f'add {len(add_tags)} tag(s) for {label}'
            )
            n_ok += 1
            continue

        try:
            # Step a: delete existing taggings before writing new values
            for tag_id, tagging_id in removals:
                ab.delete_tagging(campaign_id, tag_id, tagging_id)
                n_removals_done += 1
                logger.debug(f'  Deleted tagging {tagging_id[:8]}... for {label}')

            # Step b: post new tag values (skip if nothing to add, e.g. Clear Value)
            if add_tags:
                ab.update_entity_with_tags(campaign_id, entity_id, add_tags)
                n_tags_added += len(add_tags)
                logger.debug(f'  Added {len(add_tags)} tag(s) for {label}')

            n_ok += 1

        except Exception as e:
            logger.error(f'  ERROR updating {label}: {e}')
            n_err += 1

    logger.info(
        f'update_records: done. '
        f'ok={n_ok} err={n_err} '
        f'tags_added={n_tags_added} taggings_deleted={n_removals_done}'
    )


def insert_new_records(
    bq: BigQueryConnector,
    ab: Optional[ActionBuilderConnector],
    campaign_filter: Optional[str],
    dry_run: bool,
    limit: Optional[int],
) -> None:
    """
    Insert genuinely new entities into ActionBuilder.

    Reads actionbuilder_sync.deduplicated_names_to_load. Uses campaign_interact_id
    from each row (derived from the entity's state) for the API call.
    Pass --campaign to process only one state's records.
    """
    logger.info('insert_new_records: fetching rows from deduplicated_names_to_load...')
    sql = f'SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.deduplicated_names_to_load`'
    if limit:
        sql += f' LIMIT {limit}'
    rows = _query(bq, sql)

    if not rows:
        logger.info('insert_new_records: no rows to process')
        return

    logger.info(f'insert_new_records: {len(rows)} entities to consider')
    n_ok = n_err = n_skip = 0

    for row in rows:
        campaign_id = row.get('campaign_interact_id')
        name = (
            f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
        )
        label = (
            f'{name!r} '
            f'email={row.get("email") or "(none)"} '
            f'state={row.get("state") or "?"}'
        )

        if not campaign_id:
            logger.warning(f'  Skipping {label}: no campaign_interact_id (state has no AB campaign)')
            n_skip += 1
            continue

        if campaign_filter and campaign_id != campaign_filter:
            n_skip += 1
            continue

        # Build OSDI person_data from contact fields
        person_data: Dict[str, Any] = {}

        if row.get('first_name'):
            person_data['given_name'] = row['first_name']
        if row.get('last_name'):
            person_data['family_name'] = row['last_name']
        if row.get('email'):
            person_data['email_addresses'] = [
                {'address': row['email'], 'primary': True}
            ]
        if row.get('phone_number'):
            person_data['phone_numbers'] = [
                {'number': row['phone_number'], 'primary': True}
            ]
        postal: Dict[str, str] = {}
        if row.get('state'):
            postal['region'] = row['state']
        if row.get('zip_code'):
            postal['postal_code'] = str(row['zip_code'])
        if postal:
            person_data['postal_addresses'] = [postal]

        # Build add_tags from participation value columns
        add_tags = []
        for col in INSERT_TAG_FIELDS:
            tag = _build_insert_tag(col, row.get(col))
            if tag:
                add_tags.append(tag)

        if dry_run:
            logger.info(
                f'  [DRY-RUN] Would insert entity {label} '
                f'into campaign {campaign_id[:8]}... '
                f'with {len(add_tags)} tag(s)'
            )
            n_ok += 1
            continue

        try:
            ab.insert_entity(campaign_id, person_data, add_tags or None)
            logger.debug(f'  Inserted {label} (campaign={campaign_id[:8]}...)')
            n_ok += 1
        except Exception as e:
            logger.error(f'  ERROR inserting {label}: {e}')
            n_err += 1

    logger.info(f'insert_new_records: done. ok={n_ok} err={n_err} skipped={n_skip}')


def remove_records(
    bq: BigQueryConnector,
    ab: Optional[ActionBuilderConnector],
    campaign_filter: Optional[str],
    dry_run: bool,
    limit: Optional[int],
) -> None:
    """
    Delete duplicate entities from ActionBuilder.

    Reads actionbuilder_sync.dedup_candidates and calls delete_person for each
    delete_interact_id, using the per-row campaign_interact_id.
    """
    logger.info('remove_records: fetching rows from dedup_candidates...')
    sql = (
        f'SELECT delete_interact_id, delete_first_name, delete_last_name, '
        f'dedup_tier, campaign_interact_id '
        f'FROM `{BQ_PROJECT}.{BQ_DATASET}.dedup_candidates`'
    )
    if limit:
        sql += f' LIMIT {limit}'
    rows = _query(bq, sql)

    if not rows:
        logger.info('remove_records: no rows to process')
        return

    logger.info(f'remove_records: {len(rows)} entities to consider')
    n_ok = n_err = n_skip = 0

    for row in rows:
        entity_id = str(row['delete_interact_id'])
        campaign_id = row.get('campaign_interact_id')
        name = (
            f"{row.get('delete_first_name', '')} "
            f"{row.get('delete_last_name', '')}".strip()
        )
        tier = row.get('dedup_tier', '?')
        label = f'{entity_id[:8]}... ({name!r}, tier={tier})'

        if not campaign_id:
            logger.warning(f'  Skipping {label}: entity has no active campaign')
            n_skip += 1
            continue

        if campaign_filter and campaign_id != campaign_filter:
            n_skip += 1
            continue

        if dry_run:
            logger.info(f'  [DRY-RUN] Would delete entity {label} (campaign={campaign_id[:8]}...)')
            n_ok += 1
            continue

        try:
            ab.delete_person(campaign_id, entity_id)
            logger.debug(f'  Deleted {label}')
            n_ok += 1
        except Exception as e:
            logger.error(f'  ERROR deleting {label}: {e}')
            n_err += 1

    logger.info(f'remove_records: done. ok={n_ok} err={n_err} skipped={n_skip}')


def prepare_email_data(
    bq: BigQueryConnector,
    ab: Optional[ActionBuilderConnector],
    campaign_filter: Optional[str],
    dry_run: bool,
    limit: Optional[int],
) -> None:
    """
    Add secondary emails to keeper entities before dedup deletion.

    Reads actionbuilder_sync.email_migration_needed. Each row adds one email
    address to the keeper entity via update_person, using the per-row
    campaign_interact_id.

    Run this BEFORE remove_records so migrated emails survive the deletion.
    """
    logger.info('prepare_email_data: fetching rows from email_migration_needed...')
    sql = (
        f'SELECT entity_id, email_to_add, delete_interact_id, campaign_interact_id '
        f'FROM `{BQ_PROJECT}.{BQ_DATASET}.email_migration_needed`'
    )
    if limit:
        sql += f' LIMIT {limit}'
    rows = _query(bq, sql)

    if not rows:
        logger.info('prepare_email_data: no rows to process')
        return

    logger.info(f'prepare_email_data: {len(rows)} email(s) to consider')
    n_ok = n_err = n_skip = 0

    for row in rows:
        keeper_id = str(row['entity_id'])
        email = str(row['email_to_add'])
        campaign_id = row.get('campaign_interact_id')
        label = f'keeper={keeper_id[:8]}... email={email!r}'

        if not campaign_id:
            logger.warning(f'  Skipping {label}: keeper has no active campaign')
            n_skip += 1
            continue

        if campaign_filter and campaign_id != campaign_filter:
            n_skip += 1
            continue

        if dry_run:
            logger.info(f'  [DRY-RUN] Would add email {label} (campaign={campaign_id[:8]}...)')
            n_ok += 1
            continue

        try:
            ab.update_person(
                campaign_id,
                keeper_id,
                {'email_addresses': [{'address': email, 'primary': False}]},
            )
            logger.debug(f'  Added email {label}')
            n_ok += 1
        except Exception as e:
            logger.error(f'  ERROR adding email {label}: {e}')
            n_err += 1

    logger.info(f'prepare_email_data: done. ok={n_ok} err={n_err} skipped={n_skip}')


def prepare_phone_data(
    bq: BigQueryConnector,
    ab: Optional[ActionBuilderConnector],
    campaign_filter: Optional[str],
    dry_run: bool,
    limit: Optional[int],
) -> None:
    """
    Add secondary phone numbers to keeper entities before dedup deletion.

    Reads actionbuilder_sync.phone_migration_needed. Each row adds one phone
    number to the keeper entity via update_person, using the per-row
    campaign_interact_id.

    Run this BEFORE remove_records so migrated phone numbers survive the deletion.
    """
    logger.info('prepare_phone_data: fetching rows from phone_migration_needed...')
    sql = (
        f'SELECT entity_id, phone_to_add, delete_interact_id, campaign_interact_id '
        f'FROM `{BQ_PROJECT}.{BQ_DATASET}.phone_migration_needed`'
    )
    if limit:
        sql += f' LIMIT {limit}'
    rows = _query(bq, sql)

    if not rows:
        logger.info('prepare_phone_data: no rows to process')
        return

    logger.info(f'prepare_phone_data: {len(rows)} phone(s) to consider')
    n_ok = n_err = n_skip = 0

    for row in rows:
        keeper_id = str(row['entity_id'])
        phone = str(row['phone_to_add'])
        campaign_id = row.get('campaign_interact_id')
        label = f'keeper={keeper_id[:8]}... phone={phone!r}'

        if not campaign_id:
            logger.warning(f'  Skipping {label}: keeper has no active campaign')
            n_skip += 1
            continue

        if campaign_filter and campaign_id != campaign_filter:
            n_skip += 1
            continue

        if dry_run:
            logger.info(f'  [DRY-RUN] Would add phone {label} (campaign={campaign_id[:8]}...)')
            n_ok += 1
            continue

        try:
            ab.update_person(
                campaign_id,
                keeper_id,
                {'phone_numbers': [{'number': phone, 'primary': False}]},
            )
            logger.debug(f'  Added phone {label}')
            n_ok += 1
        except Exception as e:
            logger.error(f'  ERROR adding phone {label}: {e}')
            n_err += 1

    logger.info(f'prepare_phone_data: done. ok={n_ok} err={n_err} skipped={n_skip}')


def apply_assessments(
    bq: BigQueryConnector,
    ab: Optional[ActionBuilderConnector],
    campaign_filter: Optional[str],
    dry_run: bool,
    limit: Optional[int],
) -> None:
    """
    Write automated assessment levels to ActionBuilder entities.

    Reads actionbuilder_sync.auto_assessment_rules. The view enforces the
    write policy (no existing assessment, level 0, or level 1 set by the
    API user) and only emits upgrade candidates. For each row, calls
    update_person to set action_builder:latest_assessment.
    """
    logger.info('apply_assessments: fetching rows from auto_assessment_rules...')
    sql = (
        f'SELECT entity_interact_id, campaign_interact_id, suggested_level, '
        f'qualification_reason '
        f'FROM `{BQ_PROJECT}.{BQ_DATASET}.auto_assessment_rules`'
    )
    if limit:
        sql += f' LIMIT {limit}'
    rows = _query(bq, sql)

    if not rows:
        logger.info('apply_assessments: no entities to assess')
        return

    logger.info(f'apply_assessments: {len(rows)} entities to consider')
    n_ok = n_err = n_skip = 0

    for row in rows:
        entity_id = str(row['entity_interact_id'])
        campaign_id = row.get('campaign_interact_id')
        level = row.get('suggested_level')
        reason = row.get('qualification_reason', 'unknown')
        label = (
            f'entity={entity_id[:8]}... '
            f'campaign={str(campaign_id)[:8]}... '
            f'level={level} ({reason})'
        )

        if not campaign_id:
            logger.warning(f'  Skipping {label}: no campaign')
            n_skip += 1
            continue

        if campaign_filter and str(campaign_id) != campaign_filter:
            n_skip += 1
            continue

        if dry_run:
            logger.info(f'  [DRY-RUN] Would assess {label}')
            n_ok += 1
            continue

        try:
            ab.update_person(
                str(campaign_id),
                entity_id,
                {'action_builder:latest_assessment': level},
            )
            logger.debug(f'  Assessed {label}')
            n_ok += 1
        except Exception as e:
            logger.error(f'  ERROR assessing {label}: {e}')
            n_err += 1

    logger.info(f'apply_assessments: done. ok={n_ok} err={n_err} skipped={n_skip}')


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

OPERATIONS = {
    'update_records': update_records,
    'insert_new_records': insert_new_records,
    'remove_records': remove_records,
    'prepare_email_data': prepare_email_data,
    'prepare_phone_data': prepare_phone_data,
    'apply_assessments': apply_assessments,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description='ActionBuilder sync script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        'operation',
        choices=list(OPERATIONS.keys()),
        help='Sync operation to run',
    )
    parser.add_argument(
        '--campaign',
        metavar='CAMPAIGN',
        help=(
            'Optional filter: process only rows for this campaign. '
            'Accepts a full UUID or a lowercase state name alias '
            '(e.g. "arizona", "north_carolina"). '
            'When omitted, all rows are processed using per-row campaign data from BQ.'
        ),
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Fetch data and log what WOULD happen; no API writes',
    )
    parser.add_argument(
        '--limit',
        type=int,
        metavar='N',
        help='Process only the first N rows (useful for test campaign validation)',
    )
    args = parser.parse_args()

    # Resolve state name aliases for --campaign
    campaign_filter = _resolve_campaign_arg(args.campaign)
    if args.campaign and campaign_filter != args.campaign:
        logger.info(f'--campaign {args.campaign!r} resolved to {campaign_filter}')

    # Load .env early so both BQ and AB credentials are available to ccef-connections
    load_dotenv(dotenv_path='.env')

    # Build BQ client
    bq = _make_bq_client()

    # Build AB client only when not dry-running (no API calls will be made)
    if args.dry_run:
        ab: Optional[ActionBuilderConnector] = None
        logger.info('DRY-RUN mode: no API calls will be made')
    else:
        ab = _make_ab_client()

    OPERATIONS[args.operation](bq, ab, campaign_filter, args.dry_run, args.limit)


if __name__ == '__main__':
    main()
