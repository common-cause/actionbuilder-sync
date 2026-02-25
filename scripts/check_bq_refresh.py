"""check_bq_refresh.py - Monitor BQ refresh progress after insert_new_records.

After insert_new_records runs partially or fully, BQ lags behind AB.
This script polls deduplicated_names_to_load every N minutes and logs
when the count drops, signaling that BQ has caught up with new inserts.

Context (2026-02-23):
  - 7,243 entities inserted into AB before crash
  - BQ currently shows 20,891 in deduplicated_names_to_load
  - Expected post-refresh count: ~13,648
  - Safe to re-run insert_new_records once count stabilizes below ~14,000

Usage:
    python scripts/check_bq_refresh.py              # poll every 10 min until stable
    python scripts/check_bq_refresh.py --interval 5  # poll every 5 min
    python scripts/check_bq_refresh.py --once        # single check and exit
"""

import argparse
import time
from datetime import datetime

from dotenv import load_dotenv

from ccef_connections.connectors.bigquery import BigQueryConnector

BQ_PROJECT = 'proj-tmc-mem-com'
SQL = f'SELECT COUNT(*) AS cnt FROM `{BQ_PROJECT}.actionbuilder_sync.deduplicated_names_to_load`'
# Also check the entity count in AB cleaned tables as a cross-reference
SQL_ENTITIES = f'SELECT COUNT(*) AS cnt, MAX(created_at) AS latest FROM `{BQ_PROJECT}.actionbuilder_cleaned.cln_actionbuilder__entities`'


def check(bq: BigQueryConnector) -> tuple[int, int, str]:
    feed_rows = list(bq.query(SQL))
    feed_count = feed_rows[0]['cnt'] if feed_rows else -1

    entity_rows = list(bq.query(SQL_ENTITIES))
    entity_count = entity_rows[0]['cnt'] if entity_rows else -1
    latest = str(entity_rows[0]['latest'])[:19] if entity_rows else '?'

    return feed_count, entity_count, latest


def main():
    parser = argparse.ArgumentParser(description='Monitor BQ refresh after insert_new_records')
    parser.add_argument('--interval', type=int, default=10,
                        help='Poll interval in minutes (default: 10)')
    parser.add_argument('--once', action='store_true',
                        help='Run once and exit')
    args = parser.parse_args()

    load_dotenv(dotenv_path='.env')
    bq = BigQueryConnector(project_id=BQ_PROJECT)
    bq.connect()

    prev_feed = None

    while True:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        feed_count, entity_count, latest_created = check(bq)

        change = ''
        if prev_feed is not None and feed_count != prev_feed:
            delta = prev_feed - feed_count
            change = f'  <-- DOWN {delta:,} since last check'

        print(f'[{now}]  feed={feed_count:,}  ab_entities={entity_count:,}  latest_ab_created={latest_created}{change}', flush=True)

        if feed_count < 14000:
            print('  *** Feed count below 14,000 — BQ has likely caught up. Safe to re-run insert_new_records. ***', flush=True)

        if args.once:
            break

        prev_feed = feed_count
        time.sleep(args.interval * 60)


if __name__ == '__main__':
    main()
