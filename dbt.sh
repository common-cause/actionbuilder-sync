#!/bin/bash
# Wrapper: loads .env credentials safely, then runs dbt
python run_dbt.py "$@"
