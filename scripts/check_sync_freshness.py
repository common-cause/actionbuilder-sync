"""Dead-man's check: did the nightly ActionBuilder sync actually run?

Motivation: on 2026-07-13 the Civis container image tag `latest` rolled to Python
3.14, dbt could no longer import, step 0 failed, the on-success transition halted
the workflow — and the nightly wrote NOTHING to ActionBuilder for five weeks before
anyone noticed. Upstream data stayed healthy the whole time, so nothing else looked
wrong. This check exists so that silence gets noticed the next morning.

It deliberately runs LOCALLY (Task Scheduler), not on Civis. A monitor hosted on the
platform it monitors shares fate with it: a Civis-hosted version of this check would
have been just as dead as the nightly it was supposed to be watching.

Two independent signals:

  1. VIEW FRESHNESS (primary). Nightly step 0 runs `dbt run`, which recreates all
     ~48 actionbuilder_sync views unconditionally, every night, regardless of whether
     there is any work to do. If their last_modified_time is stale, the workflow did
     not run. This is the signal that would have caught 2026-07-13 on 2026-07-14.

  2. SYNC_LOG RECENCY (secondary, warning only). Confirms writes actually reached
     ActionBuilder. Weaker than (1) because a night with genuinely no pending changes
     legitimately writes nothing, so staleness here is suspicious rather than proof.

Exit codes: 0 healthy - 1 stale/unhealthy (alert) - 2 the check itself failed.
"""
import os
import sys
from datetime import timedelta

from dotenv import load_dotenv
from ccef_connections.connectors.bigquery import BigQueryConnector

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Anchor to the scheduled window, not a rolling age. "Views rebuilt in the last N
# hours" is satisfied by anyone running `bash dbt.sh run` locally during the day,
# which would mask a nightly that never ran. Instead require the rebuild to have
# happened AFTER the nightly was due to start: 22:00 ET = 02:00 UTC (EDT) or
# 03:00 UTC (EST), so 02:00 UTC today is the floor either way.
NIGHTLY_START_UTC_HOUR = 2
# Do not judge before the run has had a chance to finish (it can legitimately run
# long - the 2026-08-18 catch-up took 17h). Intended schedule is 09:00 ET / 13:00 UTC.
TOO_EARLY_UTC_HOUR = 8
# More lenient: a no-work night writes no sync_log rows at all.
LOG_STALE_HOURS = 30
# Expected steady-state is 0 after PR #2. Alert only on a real spike.
ERROR_SPIKE = 50


def main() -> int:
    load_dotenv(dotenv_path=os.path.join(PROJECT_DIR, ".env"))
    bq = BigQueryConnector(project_id="proj-tmc-mem-com")
    bq.connect()

    row = list(bq.query(f"""
        WITH nightly AS (
          SELECT TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY),
                               INTERVAL {NIGHTLY_START_UTC_HOUR} HOUR) AS due_at
        )
        SELECT
          (SELECT due_at FROM nightly)                                AS nightly_due_at,
          EXTRACT(HOUR FROM CURRENT_TIMESTAMP())                      AS utc_hour,
          (SELECT TIMESTAMP_MILLIS(MAX(last_modified_time))
             FROM `proj-tmc-mem-com.actionbuilder_sync.__TABLES__`
             WHERE type = 2) >= (SELECT due_at FROM nightly)          AS views_rebuilt_since_due,
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),
            (SELECT TIMESTAMP_MILLIS(MAX(last_modified_time))
             FROM `proj-tmc-mem-com.actionbuilder_sync.__TABLES__`
             WHERE type = 2), HOUR)                                   AS views_age_hours,
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),
            (SELECT MAX(executed_at) FROM actionbuilder_sync.sync_log), HOUR)
                                                                       AS log_age_hours,
          (SELECT COUNT(*) FROM actionbuilder_sync.sync_log
            WHERE executed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
                                                                       AS ops_24h,
          (SELECT COUNTIF(status != 'ok') FROM actionbuilder_sync.sync_log
            WHERE executed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
                                                                       AS errors_24h,
          (SELECT COUNT(*) FROM actionbuilder_sync.phantom_tag_writes) AS phantom_rows
    """))[0]

    views_age = row["views_age_hours"]
    log_age = row["log_age_hours"]
    ops = row["ops_24h"]
    errors = row["errors_24h"]
    phantom = row["phantom_rows"]
    rebuilt = row["views_rebuilt_since_due"]

    if row["utc_hour"] < TOO_EARLY_UTC_HOUR:
        print(f"Too early to judge (UTC hour {row['utc_hour']} < {TOO_EARLY_UTC_HOUR}); "
              f"the nightly may still be running. No verdict.")
        return 0

    problems, warnings = [], []

    if not rebuilt:
        problems.append(
            f"dbt views were NOT rebuilt since the nightly was due "
            f"({row['nightly_due_at']}); newest rebuild is {views_age}h old. "
            f"Step 0 did not run, so the whole workflow is halted. "
            f"Check Civis workflow #119217, and confirm the container image tag is "
            f"still pinned (8.5.0), not 'latest'."
        )
    if log_age is None or log_age > LOG_STALE_HOURS:
        warnings.append(
            f"no sync_log writes for {log_age}h (limit {LOG_STALE_HOURS}h). "
            f"Could be a genuinely no-work night; suspicious if it repeats."
        )
    if errors > ERROR_SPIKE:
        problems.append(f"{errors} failed operations in the last 24h (expected ~0).")
    if phantom:
        problems.append(
            f"phantom_tag_writes has {phantom} rows - tag writes are returning 200 "
            f"and silently dropping. Check per-campaign value activation."
        )

    print(f"views rebuilt : {views_age}h ago")
    print(f"last sync_log : {log_age}h ago")
    print(f"ops (24h)     : {ops:,}  errors: {errors}")
    print(f"phantom writes: {phantom}")

    if problems:
        print("\nUNHEALTHY")
        for p in problems:
            print(f"  ! {p}")
        for w in warnings:
            print(f"  ~ {w}")
        return 1

    if warnings:
        print("\nHEALTHY (with warnings)")
        for w in warnings:
            print(f"  ~ {w}")
        return 0

    print("\nHEALTHY - nightly ran, writes landed, no error spike.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:                      # the check itself broke
        print(f"CHECK FAILED TO RUN: {exc}")
        sys.exit(2)
