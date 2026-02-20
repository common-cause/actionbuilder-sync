#!/usr/bin/env python3
"""Wrapper that loads .env, writes the BQ credential to a temp file, then runs dbt."""
import os
import sys
import tempfile
import subprocess
from dotenv import load_dotenv

load_dotenv()

cred_json = os.environ.get('BIGQUERY_API_CREDENTIALS_PASSWORD', '')
if not cred_json:
    print("ERROR: BIGQUERY_API_CREDENTIALS_PASSWORD not set in .env", file=sys.stderr)
    sys.exit(1)

with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    f.write(cred_json)
    keyfile_path = f.name

try:
    env = os.environ.copy()
    env['BIGQUERY_KEYFILE_PATH'] = keyfile_path
    result = subprocess.run(['dbt'] + sys.argv[1:], env=env)
    sys.exit(result.returncode)
finally:
    os.unlink(keyfile_path)
