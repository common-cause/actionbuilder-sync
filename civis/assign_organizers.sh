#!/usr/bin/env bash
# Civis entrypoint — AB Organizing Team Assign Organizers (nightly workflow step 7).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/assign_organizers.sh
# Edit this file (not the Civis UI) to change setup/run steps.

# Pinned to a ccef-connections release tag — bump deliberately when upgrading.
pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.2.0"

# TEMPORARY --organizer hold (2026-07-29): Tiffany Rubio (unstaffed states) is in the
# seed, but her feed population contains ~70 duplicate entity pairs from the
# organizing_team_inserts double-insert bug (fixed in the model 2026-07-29; entities
# still need cleanup). Held back so the nightly doesn't wire connections to both
# halves of each pair. REMOVE the flag after the campaign-26 duplicate removal runs.
python app/scripts/sync.py assign_organizers --organizer "Carlos,Luana,Rommel,Lamair" --delay 0.3
