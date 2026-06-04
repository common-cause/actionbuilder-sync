#!/usr/bin/env bash
# Civis entrypoint — remove duplicate entities (on-demand; completed March 2026,
# retained for future use).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/remove_duplicate_entities.sh
# Edit this file (not the Civis UI) to change setup/run steps.

# Pinned to a ccef-connections release tag — bump deliberately when upgrading.
pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.2.0"

DELAY="--delay 0.3"

# Step 1: Migrate secondary emails to keeper entities before deletion
python app/scripts/sync.py prepare_email_data $DELAY

# Step 2: Migrate secondary phone numbers to keeper entities before deletion
python app/scripts/sync.py prepare_phone_data $DELAY

# Step 3: Delete duplicate entities, campaign by campaign
python app/scripts/sync.py remove_records --campaign arizona $DELAY
python app/scripts/sync.py remove_records --campaign california $DELAY
python app/scripts/sync.py remove_records --campaign colorado $DELAY
python app/scripts/sync.py remove_records --campaign florida $DELAY
python app/scripts/sync.py remove_records --campaign georgia $DELAY
python app/scripts/sync.py remove_records --campaign hawaii $DELAY
python app/scripts/sync.py remove_records --campaign illinois $DELAY
python app/scripts/sync.py remove_records --campaign indiana $DELAY
python app/scripts/sync.py remove_records --campaign maryland $DELAY
python app/scripts/sync.py remove_records --campaign massachusetts $DELAY
python app/scripts/sync.py remove_records --campaign michigan $DELAY
python app/scripts/sync.py remove_records --campaign minnesota $DELAY
python app/scripts/sync.py remove_records --campaign nebraska $DELAY
python app/scripts/sync.py remove_records --campaign new_mexico $DELAY
python app/scripts/sync.py remove_records --campaign new_york $DELAY
python app/scripts/sync.py remove_records --campaign north_carolina $DELAY
python app/scripts/sync.py remove_records --campaign ohio $DELAY
python app/scripts/sync.py remove_records --campaign oregon $DELAY
python app/scripts/sync.py remove_records --campaign pennsylvania $DELAY
python app/scripts/sync.py remove_records --campaign rhode_island $DELAY
python app/scripts/sync.py remove_records --campaign texas $DELAY
python app/scripts/sync.py remove_records --campaign wisconsin $DELAY
