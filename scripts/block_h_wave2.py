#!/usr/bin/env python
"""
Block H wave 2 — migrate the human taggings off two legacy fields, then retire them.

Wave 1 (2026-08-20) retired the ten legacy fields that had zero taggings. The next
seven legacy fields carry 136 human taggings between them, and most of those are
blocked on a human decision: `Action Team Opt-In` is still in active use by PA,
the D5 drops (cats 2/13) are awaiting a courtesy ping, and the Nebraska pair needs
the NE team to confirm the District-8 collapse.

Two fields are NOT blocked — their values map 1:1 onto the new universal Interests
fields and nothing has touched them since Nov 2025:

    Activism > Volunteer Activity Interest (cat 15, 19 taggings)
        -> Interests > Volunteer Interests (cat 40)
    Issues > Issue Bucket Interest (cat 11, 9 taggings)
        -> Interests > Issue Interests (cat 41)

This script moves those 28. Three subcommands, run in order:

    add     copy each legacy tagging forward onto the new universal field
    delete  remove the legacy taggings (only safe once `add` has been verified)
    report  read the current state of both taxonomies back out of the live API

⚠️ `add` is effectively one-way: cats 40/41 are UNIVERSAL and universal taggings
are API-undeletable (see UNIVERSAL_TAG_IDS_NO_DELETE in sync.py). The mapping below
is 1:1 with no judgement calls, but check it before running live.

The work list is read from BQ and re-verified against the live API per entity —
a tagging is only acted on when its name AND its tag interact_id both match, the
same discipline as remove_list_taggings. That matters twice over here: the two
taxonomies deliberately run similar names side by side, and `taggable_logbook`
never reflects our own deletes, so BQ alone would re-offer already-migrated rows.

No entity identifiers are hardcoded — they come from BQ at runtime, so nothing
row-level lands in the repo.
"""

import argparse
import logging
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent))

from sync import (  # noqa: E402
    BQ_DATASET,
    BQ_PROJECT,
    SyncLogger,
    _make_ab_client,
    _make_bq_client,
    _query,
)

logger = logging.getLogger('block_h_wave2')

# legacy tag interact_id -> where that value now lives in the new taxonomy.
# Verified against live AB 2026-08-20 via getTags on cats 40/41 — every target
# exists and is unarchived. Legacy ids come from cln_actionbuilder__tags.
MIGRATIONS: Dict[str, Dict[str, str]] = {
    # Activism > Volunteer Activity Interest (cat 15) -> Interests > Volunteer Interests (cat 40)
    '0fdb02dd-c691-4027-8ea4-7fc2f3b87f7a': {
        'legacy_name': 'Phone Banking',
        'section': 'Interests',
        'field': 'Volunteer Interests',
        'value': 'Interest: Phone Banking',
        'tag_id': '2b6c8fcb-74f7-4ad5-8314-9d545a28094f',
    },
    '0aee65af-10f5-4b37-b8b6-617110d93913': {
        'legacy_name': 'Poll Monitoring',
        'section': 'Interests',
        'field': 'Volunteer Interests',
        'value': 'Interest: Poll Monitoring',
        'tag_id': 'a680afe3-6210-4f18-9629-bf38eef4f1d0',
    },
    '0bf57f5a-36b1-4b3c-87aa-294c4be02584': {
        'legacy_name': 'Electoral Count Monitoring',
        'section': 'Interests',
        'field': 'Volunteer Interests',
        'value': 'Interest: Electoral Count Monitoring',
        'tag_id': 'ab61be6c-079f-4e38-9c1a-443553a0f132',
    },
    'ee06fbc9-d2e8-4d36-b549-8f403c41a517': {
        'legacy_name': 'Event Volunteer',
        'section': 'Interests',
        'field': 'Volunteer Interests',
        'value': 'Interest: Event Volunteering',
        'tag_id': '8eb82545-44df-4576-8d7f-1d73b0baab69',
    },
    # Issues > Issue Bucket Interest (cat 11) -> Interests > Issue Interests (cat 41)
    '002d185b-0b7f-4cb3-b21b-a2b15de0a4fc': {
        'legacy_name': 'Accountability and Anti-Corruption',
        'section': 'Interests',
        'field': 'Issue Interests',
        'value': 'Issue: Accountability & Anti-Corruption',
        'tag_id': '1fa186fb-8196-4e3a-afef-861fd13c562c',
    },
    '5834f6dd-fdd5-4bb2-8c71-26f15f9f3e0d': {
        'legacy_name': 'Voting and Fair Representation',
        'section': 'Interests',
        'field': 'Issue Interests',
        'value': 'Issue: Voting & Fair Representation',
        'tag_id': '89f8d2b2-2de3-4b49-98f5-f9c78d2a2cae',
    },
    'a98528bf-9a39-428e-b378-be5810b45d88': {
        'legacy_name': 'Civil Rights and Civil Liberties',
        'section': 'Interests',
        'field': 'Issue Interests',
        'value': 'Issue: Civil Rights & Civil Liberties',
        'tag_id': 'b529f7dc-9a1e-418c-87d7-411a8e10b398',
    },
}

WORKLIST_SQL = f"""
SELECT DISTINCT
       e.interact_id AS entity_interact_id,
       c.interact_id AS campaign_interact_id,
       c.name        AS campaign_name,
       t.interact_id AS legacy_tag_interact_id,
       t.name        AS legacy_tag_name
FROM `{BQ_PROJECT}.actionbuilder_cleaned.cln_actionbuilder__taggable_logbook` l
JOIN `{BQ_PROJECT}.actionbuilder_cleaned.cln_actionbuilder__tags` t
  ON t.id = l.tag_id
JOIN `{BQ_PROJECT}.actionbuilder_cleaned.cln_actionbuilder__entities` e
  ON e.id = l.taggable_id
JOIN `{BQ_PROJECT}.actionbuilder_cleaned.cln_actionbuilder__campaigns` c
  ON c.id = l.campaign_id
WHERE t.tag_category_id IN (11, 15)
  AND l.deleted_at IS NULL
ORDER BY campaign_name, legacy_tag_name, entity_interact_id
"""


def _live_taggings(ab, campaign_id: str, entity_id: str) -> List[Dict[str, Any]]:
    """List an entity's taggings in one campaign, as returned by the OSDI API."""
    return ab.list_person_taggings(campaign_id, entity_id)


def _find_legacy(taggings: List[Dict[str, Any]], name: str, tag_id: str) -> List[str]:
    """
    Return tagging interact_ids matching BOTH the expected value name and tag id.

    Name alone is not a safe key — the migration deliberately runs similar names
    across two taxonomies, and a "Storytelling" collision already exists.
    """
    out = []
    for t in taggings:
        if t.get('action_builder:name') != name:
            continue
        href = t.get('_links', {}).get('osdi:tag', {}).get('href', '')
        if tag_id not in href:
            continue
        ids = t.get('identifiers', [])
        if ids and str(ids[0]).startswith('action_builder:'):
            out.append(str(ids[0])[len('action_builder:'):])
    return out


def _has_value(taggings: List[Dict[str, Any]], value: str, tag_id: str) -> bool:
    """True when the entity already carries the new-taxonomy value."""
    return bool(_find_legacy(taggings, value, tag_id))


def _worklist(bq, limit: Optional[int]) -> List[Dict[str, Any]]:
    rows = _query(bq, WORKLIST_SQL)
    rows = [r for r in rows if str(r['legacy_tag_interact_id']) in MIGRATIONS]
    if limit:
        rows = rows[:limit]
    return rows


def run_add(bq, ab, dry_run: bool, limit: Optional[int], sync_logger, delay: float) -> None:
    """Copy each legacy tagging forward onto its new universal home."""
    rows = _worklist(bq, limit)
    logger.info(f'add: {len(rows)} candidate tagging(s) from BQ')

    n_added = n_present = n_absent = n_err = 0

    for row in rows:
        entity_id = str(row['entity_interact_id'])
        campaign_id = str(row['campaign_interact_id'])
        legacy_tag = str(row['legacy_tag_interact_id'])
        m = MIGRATIONS[legacy_tag]
        label = f"{row['campaign_name']}/{entity_id[:8]}... {m['legacy_name']!r}"

        try:
            taggings = _live_taggings(ab, campaign_id, entity_id)
        except Exception as e:
            logger.error(f'  ERROR listing {label}: {e}')
            n_err += 1
            continue

        # The legacy tagging must actually be live — BQ shows ghosts.
        if not _find_legacy(taggings, m['legacy_name'], legacy_tag):
            logger.info(f'  SKIP {label}: legacy tagging not present in AB (BQ ghost)')
            n_absent += 1
            continue

        if _has_value(taggings, m['value'], m['tag_id']):
            logger.info(f"  ALREADY {label} -> {m['value']!r}")
            n_present += 1
            continue

        tag = {
            'action_builder:section': m['section'],
            'action_builder:field': m['field'],
            'name': m['value'],
        }

        if dry_run:
            logger.info(f"  [DRY-RUN] {label} -> {m['value']!r}")
            n_added += 1
            continue

        try:
            ab.update_entity_with_tags(campaign_id, entity_id, [tag])
            n_added += 1
            logger.info(f"  ADDED {label} -> {m['value']!r}")
            if sync_logger:
                sync_logger.log(
                    operation='add_tagging',
                    entity_interact_id=entity_id,
                    campaign_interact_id=campaign_id,
                    status='ok',
                    tag_interact_id=m['tag_id'],
                    tag_name=m['field'],
                    value_written=m['value'],
                )
        except Exception as e:
            logger.error(f'  ERROR adding for {label}: {e}')
            n_err += 1
            if sync_logger:
                sync_logger.log(
                    'add_tagging', entity_id, campaign_id, 'error',
                    tag_interact_id=m['tag_id'], tag_name=m['field'],
                    value_written=m['value'], error_detail=str(e)[:500],
                )

        if delay:
            time.sleep(delay)

    logger.info(
        f'add: done. added={n_added} already_present={n_present} '
        f'legacy_absent={n_absent} err={n_err}'
    )


def run_delete(bq, ab, dry_run: bool, limit: Optional[int], sync_logger, delay: float) -> None:
    """
    Delete the legacy taggings — only after `add` has been verified.

    Refuses to delete a legacy tagging unless the new-taxonomy value is confirmed
    present on the same entity, so a half-finished migration cannot lose data.
    """
    rows = _worklist(bq, limit)
    logger.info(f'delete: {len(rows)} candidate tagging(s) from BQ')

    n_deleted = n_clear = n_unmigrated = n_err = 0

    for row in rows:
        entity_id = str(row['entity_interact_id'])
        campaign_id = str(row['campaign_interact_id'])
        legacy_tag = str(row['legacy_tag_interact_id'])
        m = MIGRATIONS[legacy_tag]
        label = f"{row['campaign_name']}/{entity_id[:8]}... {m['legacy_name']!r}"

        try:
            taggings = _live_taggings(ab, campaign_id, entity_id)
        except Exception as e:
            logger.error(f'  ERROR listing {label}: {e}')
            n_err += 1
            continue

        matches = _find_legacy(taggings, m['legacy_name'], legacy_tag)
        if not matches:
            n_clear += 1
            continue

        # Never drop the legacy copy before the new one is confirmed landed.
        if not _has_value(taggings, m['value'], m['tag_id']):
            logger.warning(
                f"  REFUSING {label}: new value {m['value']!r} not present — run `add` first"
            )
            n_unmigrated += 1
            continue

        for tagging_id in matches:
            if dry_run:
                logger.info(f'  [DRY-RUN] would delete {tagging_id[:8]}... for {label}')
                n_deleted += 1
                continue
            try:
                status = ab.delete_tagging(campaign_id, legacy_tag, tagging_id)
                n_deleted += 1
                logger.info(f'  DELETED {tagging_id[:8]}... for {label}')
                if sync_logger:
                    sync_logger.log(
                        operation='delete_tagging',
                        entity_interact_id=entity_id,
                        campaign_interact_id=campaign_id,
                        status=status,
                        tag_interact_id=legacy_tag,
                        tagging_interact_id=tagging_id,
                        tag_name=m['legacy_name'],
                    )
            except Exception as e:
                logger.error(f'  ERROR deleting {tagging_id[:8]}... for {label}: {e}')
                n_err += 1
                if sync_logger:
                    sync_logger.log(
                        'delete_tagging', entity_id, campaign_id, 'error',
                        tag_interact_id=legacy_tag, tagging_interact_id=tagging_id,
                        tag_name=m['legacy_name'], error_detail=str(e)[:500],
                    )

        if delay:
            time.sleep(delay)

    logger.info(
        f'delete: done. deleted={n_deleted} already_clear={n_clear} '
        f'unmigrated_refused={n_unmigrated} err={n_err}'
    )


def run_report(bq, ab, dry_run: bool, limit: Optional[int], sync_logger, delay: float) -> None:
    """Read both taxonomies back out of the live API for every affected entity."""
    rows = _worklist(bq, limit)
    seen = {}
    for row in rows:
        key = (str(row['entity_interact_id']), str(row['campaign_interact_id']))
        seen.setdefault(key, str(row['campaign_name']))

    logger.info(f'report: reading {len(seen)} entity/campaign pair(s) from the live API')
    legacy_total = new_total = 0

    for (entity_id, campaign_id), campaign_name in sorted(seen.items(), key=lambda kv: kv[1]):
        try:
            taggings = _live_taggings(ab, campaign_id, entity_id)
        except Exception as e:
            logger.error(f'  ERROR listing {entity_id[:8]}...: {e}')
            continue
        legacy, new = [], []
        for legacy_tag, m in MIGRATIONS.items():
            if _find_legacy(taggings, m['legacy_name'], legacy_tag):
                legacy.append(m['legacy_name'])
            if _has_value(taggings, m['value'], m['tag_id']):
                new.append(m['value'])
        legacy_total += len(legacy)
        new_total += len(new)
        logger.info(
            f'  {campaign_name}/{entity_id[:8]}... legacy={sorted(legacy)} new={sorted(new)}'
        )

    logger.info(f'report: legacy taggings still live={legacy_total} new taggings={new_total}')


PHASES = {'add': run_add, 'delete': run_delete, 'report': run_report}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('phase', choices=list(PHASES.keys()))
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--delay', type=float, default=0.3)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    load_dotenv(dotenv_path='.env')

    run_id = f'block_h_wave2_{args.phase}_{uuid.uuid4().hex[:8]}'
    logger.info(f'Run ID: {run_id}')

    bq = _make_bq_client()
    ab = _make_ab_client()  # every phase reads the API, even under --dry-run
    if args.dry_run:
        logger.info('DRY-RUN: read-only API calls allowed; no writes will be made')

    sync_logger = SyncLogger(bq, run_id, dry_run=args.dry_run)
    PHASES[args.phase](bq, ab, args.dry_run, args.limit, sync_logger, args.delay)
    sync_logger.flush()


if __name__ == '__main__':
    main()
