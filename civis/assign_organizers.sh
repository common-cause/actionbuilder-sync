#!/usr/bin/env bash
# Civis entrypoint — AB Organizing Team Assign Organizers (nightly workflow step 7).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/assign_organizers.sh
# Edit this file (not the Civis UI) to change setup/run steps.

# Pinned to a ccef-connections release tag — bump deliberately when upgrading.
pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.2.0"

# TEMPORARY --organizer filter: Lamair Bryan has no AB account yet, so his members
# (states CA/HI/MA/MI/NC) are held back to avoid nightly 404 errors. When his entity
# exists and is connected to campaign 26, REMOVE the --organizer flag entirely so all
# four organizers (and any future new members in Lamair's states) are assigned.
# See task: "Assign Lamair's members once his account exists".
python app/scripts/sync.py assign_organizers --organizer "Carlos,Luana,Rommel" --delay 0.3
