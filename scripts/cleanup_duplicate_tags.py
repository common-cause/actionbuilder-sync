"""cleanup_duplicate_tags.py - Delete duplicate tagging records from ActionBuilder.

When the AB->BQ replication lags, the sync script writes the same tag repeatedly
because it can't see tags already in AB. This script identifies all cases where
the same tag is applied more than once to the same entity in the same campaign,
keeps the most recent write, and deletes all older duplicates via the AB API.

Usage:
    python scripts/cleanup_duplicate_tags.py [--campaign CAMPAIGN] [--dry-run] [--limit N]

    --campaign   Optional filter: only clean duplicates in this campaign.
                 Accepts a full UUID or a lowercase state name alias (e.g. "arizona").
    --dry-run    Fetch data and log what WOULD happen; no API writes.
    --limit N    Process only the first N duplicate records.

Credentials (in .env):
    BIGQUERY_CREDENTIALS_PASSWORD        Service account JSON (already present)
    ACTION_BUILDER_CREDENTIALS_PASSWORD  JSON: {"api_token": "...", "subdomain": "..."}
"""

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
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
# BigQuery dataset / project
# ---------------------------------------------------------------------------
BQ_PROJECT = 'proj-tmc-mem-com'

# ---------------------------------------------------------------------------
# Known campaign name aliases (shared with sync.py)
# ---------------------------------------------------------------------------
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


def _resolve_campaign_arg(value: Optional[str]) -> Optional[str]:
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


# ---------------------------------------------------------------------------
# SyncLogger (duplicated from sync.py — both scripts are standalone)
# ---------------------------------------------------------------------------

class SyncLogger:
    """
    Batches sync API call results and flushes them to actionbuilder_sync.sync_log.

    Provides an audit trail of tagging operations for BQ evidence and future
    tag-state reconstruction when taggable_logbook replication is stale.

    In --dry-run mode, log() and flush() are both no-ops.
    """

    TABLE = 'actionbuilder_sync.sync_log'
    BATCH_SIZE = 100

    def __init__(
        self, bq: BigQueryConnector, run_id: str, dry_run: bool = False
    ) -> None:
        self._bq = bq
        self._run_id = run_id
        self._dry_run = dry_run
        self._pending: List[Dict[str, Any]] = []

    def log(
        self,
        operation: str,
        entity_interact_id: Optional[str],
        campaign_interact_id: Optional[str],
        status: str,
        person_id: Optional[str] = None,
        tag_interact_id: Optional[str] = None,
        tagging_interact_id: Optional[str] = None,
        tag_name: Optional[str] = None,
        value_written: Optional[str] = None,
        error_detail: Optional[str] = None,
    ) -> None:
        """Append one log row. Auto-flushes when BATCH_SIZE is reached."""
        if self._dry_run:
            return
        self._pending.append({
            'run_id': self._run_id,
            'operation': operation,
            'entity_interact_id': entity_interact_id,
            'campaign_interact_id': campaign_interact_id,
            'person_id': person_id,
            'tag_interact_id': tag_interact_id,
            'tagging_interact_id': tagging_interact_id,
            'tag_name': tag_name,
            'value_written': value_written,
            'executed_at': datetime.now(timezone.utc).isoformat(),
            'status': status,
            'error_detail': error_detail,
        })
        if len(self._pending) >= self.BATCH_SIZE:
            self.flush()

    def flush(self) -> None:
        """Write all pending rows to BQ. Logs a warning on failure but does not raise."""
        if not self._pending or self._dry_run:
            return
        batch = self._pending[:]
        self._pending = []
        try:
            self._bq.insert_rows(self.TABLE, batch)
            logger.debug(f'SyncLogger: flushed {len(batch)} rows to sync_log')
        except Exception as e:
            logger.warning(f'SyncLogger: failed to flush {len(batch)} rows: {e}')


# ---------------------------------------------------------------------------
# Main operation
# ---------------------------------------------------------------------------

def cleanup_duplicate_tags(
    bq: BigQueryConnector,
    ab: Optional[ActionBuilderConnector],
    campaign_filter: Optional[str],
    dry_run: bool,
    limit: Optional[int],
    delay: float = 0.0,
    sync_logger: Optional[SyncLogger] = None,
) -> None:
    """
    Find all tagging records where the same tag has been applied more than once
    to the same entity in the same campaign. Keep the most recent (by updated_at),
    delete all older duplicates via the AB API.
    """
    logger.info('cleanup_duplicate_tags: querying BQ for duplicate taggings...')

    campaign_clause = ''
    if campaign_filter:
        campaign_clause = f"AND c.interact_id = '{campaign_filter}'"

    limit_clause = f'LIMIT {limit}' if limit else ''

    sql = f"""
        SELECT
            tl.interact_id      AS tagging_interact_id,
            t.interact_id       AS tag_interact_id,
            t.name              AS tag_name,
            c.interact_id       AS campaign_interact_id,
            tl.taggable_id      AS entity_id,
            e.interact_id       AS entity_interact_id
        FROM (
            SELECT
                interact_id,
                tag_id,
                campaign_id,
                taggable_id,
                ROW_NUMBER() OVER (
                    PARTITION BY taggable_id, tag_id, campaign_id
                    ORDER BY updated_at DESC
                ) AS rn
            FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook
            WHERE deleted_at IS NULL
              AND available = TRUE
              AND taggable_type = 'Entity'
        ) tl
        JOIN actionbuilder_cleaned.cln_actionbuilder__tags t
            ON t.id = tl.tag_id
        JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
            ON c.id = tl.campaign_id
        JOIN actionbuilder_cleaned.cln_actionbuilder__entities e
            ON e.id = tl.taggable_id
        WHERE tl.rn > 1
          {campaign_clause}
        ORDER BY tl.taggable_id, tl.tag_id
        {limit_clause}
    """

    rows = _query(bq, sql)

    if not rows:
        logger.info('cleanup_duplicate_tags: no duplicate taggings found')
        return

    logger.info(f'cleanup_duplicate_tags: {len(rows)} excess tagging(s) to delete')

    n_ok = n_err = 0

    for i, row in enumerate(rows, 1):
        tagging_id = str(row['tagging_interact_id'])
        tag_id = str(row['tag_interact_id'])
        tag_name = str(row['tag_name'])
        campaign_id = str(row['campaign_interact_id'])
        label = (
            f'tagging={tagging_id[:8]}... '
            f'tag="{tag_name}" '
            f'campaign={campaign_id[:8]}...'
        )

        if dry_run:
            logger.info(f'  [DRY-RUN] Would delete {label}')
            n_ok += 1
            continue

        try:
            status = ab.delete_tagging(campaign_id, tag_id, tagging_id)
            n_ok += 1
            if sync_logger:
                sync_logger.log(
                    operation='delete_tagging',
                    entity_interact_id=str(row['entity_interact_id']),
                    campaign_interact_id=campaign_id,
                    status=status,
                    tag_interact_id=tag_id,
                    tagging_interact_id=tagging_id,
                    tag_name=tag_name,
                )
            if i % 500 == 0:
                logger.info(f'  Progress: {i}/{len(rows)} (ok={n_ok} err={n_err})')
            else:
                logger.debug(f'  Deleted {label}')
            if delay:
                time.sleep(delay)
        except Exception as e:
            logger.error(f'  ERROR deleting {label}: {e}')
            n_err += 1

    logger.info(
        f'cleanup_duplicate_tags: done. '
        f'deleted={n_ok} err={n_err}'
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Delete duplicate tagging records from ActionBuilder.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--campaign',
        metavar='CAMPAIGN',
        help=(
            'Optional filter: clean only this campaign. '
            'Accepts a full UUID or a lowercase state name alias '
            '(e.g. "arizona", "north_carolina").'
        ),
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Fetch data and log what WOULD happen; no API writes.',
    )
    parser.add_argument(
        '--limit',
        type=int,
        metavar='N',
        help='Process only the first N duplicate records.',
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.0,
        metavar='SECONDS',
        help='Seconds to sleep between deletes (default 0). Use 0.3 on Civis to avoid rate limits.',
    )
    args = parser.parse_args()

    campaign_filter = _resolve_campaign_arg(args.campaign)
    if args.campaign and campaign_filter != args.campaign:
        logger.info(f'--campaign {args.campaign!r} resolved to {campaign_filter}')

    load_dotenv(dotenv_path='.env')

    run_id = str(uuid.uuid4())
    logger.info(f'Run ID: {run_id}')

    bq = _make_bq_client()

    if args.dry_run:
        ab: Optional[ActionBuilderConnector] = None
        logger.info('DRY-RUN mode: no API calls will be made')
    else:
        ab = _make_ab_client()

    sync_logger = SyncLogger(bq, run_id, dry_run=args.dry_run)
    cleanup_duplicate_tags(bq, ab, campaign_filter, args.dry_run, args.limit, args.delay,
                           sync_logger=sync_logger)
    sync_logger.flush()


if __name__ == '__main__':
    main()
