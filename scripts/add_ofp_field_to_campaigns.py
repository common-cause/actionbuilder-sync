"""
Add "Organizing for Power" tag responses to all active ActionBuilder campaigns.

The field (Activism > Organizing for Power, standard multiselect) was created
manually in the AB GUI.  This script adds the four response tags to every
active campaign so the sync pipeline can write values.

Idempotent: skips any campaign that already has a tag with the same name.
Safe to re-run.

Usage:
    python scripts/add_ofp_field_to_campaigns.py [--dry-run]
"""
import argparse

from dotenv import load_dotenv
from ccef_connections.connectors.action_builder import ActionBuilderConnector

OFP_TAGS = [
    {
        "name": "Organizing Basics",
        "action_builder:section": "Activism",
        "action_builder:field": "Organizing for Power",
        "action_builder:field_type": "standard",
        "action_builder:allow_multiple_responses": True,
    },
    {
        "name": "Storytelling",
        "action_builder:section": "Activism",
        "action_builder:field": "Organizing for Power",
        "action_builder:field_type": "standard",
        "action_builder:allow_multiple_responses": True,
    },
    {
        "name": "Relational Organizing",
        "action_builder:section": "Activism",
        "action_builder:field": "Organizing for Power",
        "action_builder:field_type": "standard",
        "action_builder:allow_multiple_responses": True,
    },
    {
        "name": "Rapid Response Basics",
        "action_builder:section": "Activism",
        "action_builder:field": "Organizing for Power",
        "action_builder:field_type": "standard",
        "action_builder:allow_multiple_responses": True,
    },
]


def list_tags_for_campaign(ab, campaign_id):
    """Return all tags for a campaign using the correct embedded key."""
    all_tags = []
    page = 1
    while True:
        raw = ab._request(
            "GET", f"/campaigns/{campaign_id}/tags",
            params={"page": page, "per_page": 25}
        )
        all_tags.extend(raw.get("_embedded", {}).get("osdi:tags", []))
        if page >= raw.get("total_pages", 1):
            break
        page += 1
    return all_tags


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv(dotenv_path=".env")
    ab = ActionBuilderConnector()
    ab.connect()

    campaigns = ab.list_campaigns()
    active = [c for c in campaigns if not c.get("archived", False) and c.get("name") != "Test"]
    print(f"Found {len(active)} active campaigns")
    print()

    totals = {"added": 0, "skipped": 0, "errors": 0}

    for campaign in sorted(active, key=lambda c: c.get("name", "")):
        campaign_id = next(
            (i.split(":")[1] for i in campaign.get("identifiers", []) if i.startswith("action_builder:")),
            None
        )
        if not campaign_id:
            print(f"  WARNING: could not find interact_id for campaign {campaign.get('name')}")
            continue

        campaign_name = campaign.get("name", campaign_id)

        existing = list_tags_for_campaign(ab, campaign_id)
        existing_names = {t["name"] for t in existing}

        for tag_def in OFP_TAGS:
            tag_name = tag_def["name"]
            if tag_name in existing_names:
                print(f"  SKIP  {campaign_name}: {tag_name} (already present)")
                totals["skipped"] += 1
                continue

            if args.dry_run:
                print(f"  DRY   {campaign_name}: would add {tag_name}")
                totals["added"] += 1
                continue

            try:
                ab._request("POST", f"/campaigns/{campaign_id}/tags", json_body=tag_def)
                print(f"  ADD   {campaign_name}: {tag_name}")
                totals["added"] += 1
            except Exception as e:
                print(f"  ERROR {campaign_name}: {tag_name} — {e}")
                totals["errors"] += 1

    print()
    print(f"Done. Added={totals['added']}  Skipped={totals['skipped']}  Errors={totals['errors']}")


if __name__ == "__main__":
    main()
