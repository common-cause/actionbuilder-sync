"""
Time a query against the updates_needed view (or any other query).
Usage:
    python scripts/time_query.py
    python scripts/time_query.py --query "SELECT COUNT(*) FROM ..."

Prints row count and wall-clock elapsed time. Run before and after
the correlated-subquery -> CTE refactor to compare performance.
"""
import argparse
import time

from dotenv import load_dotenv
from ccef_connections.connectors.bigquery import BigQueryConnector

DEFAULT_QUERY = "SELECT * FROM actionbuilder_sync.updates_needed"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default=DEFAULT_QUERY)
    args = parser.parse_args()

    load_dotenv(dotenv_path=".env")
    bq = BigQueryConnector(project_id="proj-tmc-mem-com")
    bq.connect()

    print(f"Running: {args.query[:120]}...")
    t0 = time.perf_counter()
    rows = list(bq.query(args.query))
    elapsed = time.perf_counter() - t0

    print(f"Rows returned : {len(rows)}")
    print(f"Elapsed time  : {elapsed:.2f}s")

if __name__ == "__main__":
    main()
