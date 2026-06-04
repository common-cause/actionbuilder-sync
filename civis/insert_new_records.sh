#!/usr/bin/env bash
# Civis entrypoint — AB Inserts (nightly workflow step 1).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/insert_new_records.sh
# Edit this file (not the Civis UI) to change setup/run steps.

# Pinned to a ccef-connections release tag — bump deliberately when upgrading.
pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.2.0"

DELAY="--delay 0.3"

python app/scripts/sync.py insert_new_records --campaign michigan $DELAY
python app/scripts/sync.py insert_new_records --campaign nebraska $DELAY
python app/scripts/sync.py insert_new_records --campaign california $DELAY
python app/scripts/sync.py insert_new_records --campaign new_york $DELAY
python app/scripts/sync.py insert_new_records --campaign texas $DELAY
python app/scripts/sync.py insert_new_records --campaign pennsylvania $DELAY
python app/scripts/sync.py insert_new_records --campaign florida $DELAY
python app/scripts/sync.py insert_new_records --campaign north_carolina $DELAY
python app/scripts/sync.py insert_new_records --campaign colorado $DELAY
python app/scripts/sync.py insert_new_records --campaign ohio $DELAY
python app/scripts/sync.py insert_new_records --campaign massachusetts $DELAY
python app/scripts/sync.py insert_new_records --campaign oregon $DELAY
python app/scripts/sync.py insert_new_records --campaign illinois $DELAY
python app/scripts/sync.py insert_new_records --campaign minnesota $DELAY
python app/scripts/sync.py insert_new_records --campaign arizona $DELAY
python app/scripts/sync.py insert_new_records --campaign new_mexico $DELAY
python app/scripts/sync.py insert_new_records --campaign wisconsin $DELAY
python app/scripts/sync.py insert_new_records --campaign maryland $DELAY
python app/scripts/sync.py insert_new_records --campaign indiana $DELAY
python app/scripts/sync.py insert_new_records --campaign georgia $DELAY
python app/scripts/sync.py insert_new_records --campaign rhode_island $DELAY
python app/scripts/sync.py insert_new_records --campaign hawaii $DELAY
python app/scripts/sync.py insert_new_records --campaign virginia $DELAY
python app/scripts/sync.py insert_new_records --campaign dc $DELAY
