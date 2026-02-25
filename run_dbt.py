"""
run_dbt.py — dbt wrapper for projects using JSON credentials in .env

Problem: dbt-bigquery profiles.yml does not support parsing JSON from an env var inline
(fromjson is not available). And `source .env` fails in bash when the JSON is unquoted.

Solution: This wrapper loads .env via python-dotenv, writes the JSON to a temp keyfile,
sets BIGQUERY_KEYFILE_PATH, then execs dbt with all provided arguments.

Usage (via dbt.sh):
    bash dbt.sh run
    bash dbt.sh test
    bash dbt.sh run -s my_model
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (explicit path avoids find_dotenv AssertionError in subprocesses)
project_root = Path(__file__).parent
load_dotenv(dotenv_path=project_root / ".env")

cred_json = os.environ.get("BIGQUERY_CREDENTIALS_PASSWORD", "")
if not cred_json:
    print("ERROR: BIGQUERY_CREDENTIALS_PASSWORD not set in .env")
    sys.exit(1)

# Validate JSON before writing
try:
    json.loads(cred_json)
except json.JSONDecodeError as e:
    print(f"ERROR: BIGQUERY_CREDENTIALS_PASSWORD is not valid JSON: {e}")
    sys.exit(1)

# Write to temp file and run dbt
with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
    tmp.write(cred_json)
    tmp_path = tmp.name

try:
    env = os.environ.copy()
    env["BIGQUERY_KEYFILE_PATH"] = tmp_path
    # dbt 1.9+: --profiles-dir must come after the subcommand, not before it
    args = sys.argv[1:]
    if args:
        cmd = ["dbt", args[0], "--profiles-dir", str(project_root), *args[1:]]
    else:
        cmd = ["dbt", "--profiles-dir", str(project_root)]
    result = subprocess.run(
        cmd,
        env=env,
        cwd=project_root,
    )
    sys.exit(result.returncode)
finally:
    try:
        os.unlink(tmp_path)
    except OSError:
        pass
