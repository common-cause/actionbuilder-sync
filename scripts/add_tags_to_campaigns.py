"""
Add new tag definitions to all active ActionBuilder campaigns.

Idempotent: skips any campaign that already has a tag with the same
name in the same section/field. Safe to re-run.

Usage:
    python scripts/add_tags_to_campaigns.py [--dry-run]
"""
import argparse

from dotenv import load_dotenv
from ccef_connections.connectors.action_builder import ActionBuilderConnector

# Tags to add — must match the section/field/name created in the GUI
NEW_TAGS = [
    {
        "name": "NewMode Actions",
        "action_builder:section": "Participation",
        "action_builder:field": "Online Actions Past 6 Months",
        "action_builder:field_type": "number",
        "action_builder:allow_multiple_responses": True,
    },
    {
        "name": "Top National Action Network Activist",
        "action_builder:section": "Participation",
        "action_builder:field": "National Online Actions",
        "action_builder:field_type": "standard",
        "action_builder:allow_multiple_responses": True,
    },
    {
        "name": "Hot Prospect",
        "action_builder:section": "Engagement",
        "action_builder:field": "Prospect Identification",
        "action_builder:field_type": "standard",
        "action_builder:allow_multiple_responses": False,
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

    # Get all campaigns
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

        # Get existing tag names for this campaign
        existing = list_tags_for_campaign(ab, campaign_id)
        existing_names = {t["name"] for t in existing}

        for tag_def in NEW_TAGS:
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
