"""check_recent_inserts.py - Query AB API directly for entities created today.

Bypasses BQ entirely to check what was actually inserted into ActionBuilder.
Useful after a partial/crashed insert run to know the true current state.

Usage:
    python scripts/check_recent_inserts.py [--date YYYY-MM-DD] [--campaign ALIAS]

    --date       Date to check (default: today)
    --campaign   Limit to one state (e.g. 'arizona'). Default: all campaigns.
    --verbose    Print each entity name (not just counts)
"""

import argparse
import sys
from datetime import date, timezone, datetime

from dotenv import load_dotenv

from ccef_connections.connectors.action_builder import ActionBuilderConnector

# Same aliases as sync.py
CAMPAIGN_ALIASES = {
    'arizona':        'a41cde2c-a06f-4fed-8073-b544ca9aead7',
    'california':     'fd65be58-cce6-400f-97f8-e14adb6558d3',
    'colorado':       'c04eece0-5e68-410d-8436-7b28690d4fe0',
    'florida':        'c998a441-0cc0-405e-a3fe-1b4839ec101a',
    'georgia':        'dd6b11e3-d82a-44c5-91bb-ec516c723fd0',
    'hawaii':         '993e08fe-bdeb-460c-832c-71c1b8c19dba',
    'illinois':       'b6c5d9d8-c382-4da2-85ee-fbf6594d0a04',
    'indiana':        'af5fcde6-2b84-48c3-a8bb-7de045ede252',
    'maryland':       '16702ebe-ddc3-4c80-b832-b9f0a6881f0c',
    'massachusetts':  '51fb121f-a9c6-47a9-a27e-163d0f81b9f2',
    'michigan':       '8407578c-f147-4d50-a91e-245282bc4aa2',
    'minnesota':      'f6b17bf5-90e2-4252-8e7e-cf11ff3f83a0',
    'nebraska':       'e37684a0-1284-49b5-b4aa-855d9faa5ae2',
    'new_mexico':     'feb40677-0ed8-4a1e-9fd2-290526dc6ab1',
    'new_york':       '9f4b8be6-9baf-430d-b548-77227b787f86',
    'north_carolina': '96dca89a-61bd-49f4-87a8-4368e655f1c3',
    'ohio':           '37c5ef62-f4de-4769-ae19-624e5ae42ecd',
    'oregon':         'e8298624-3568-4d92-948b-4429e55d6271',
    'pennsylvania':   'a00b53e0-1ffb-4692-a347-58fe1ad73aa8',
    'rhode_island':   'd5c48860-3764-4020-9d21-ac6024daefa0',
    'test':           '0e41ca37-e05d-499c-943b-9d08dc8725b0',
    'texas':          'c7cf1a2b-a9e5-43dd-93a9-928d4bc979e4',
    'wisconsin':      '12951a1f-6d24-4923-ba31-d4aa6c4c3183',
}


def check_campaign(ab: ActionBuilderConnector, name: str, campaign_id: str,
                   target_date: date, verbose: bool) -> int:
    since = f"{target_date}T00:00:00"
    print(f"  {name}: querying... ", end="", flush=True)
    try:
        # Use _paginate directly with the correct embedded key.
        # The AB API returns 'osdi:people', not 'action_builder:entities'
        # (connector bug — list_people uses the wrong key and always returns []).
        params = {"filter": f"modified_date gt '{since}'"}
        entities = ab._paginate(
            f"/campaigns/{campaign_id}/people", "osdi:people", params
        )
    except Exception as e:
        print(f"ERROR: {e}")
        return 0

    # Filter to created on target_date (modified_since may include older updated entities)
    created_today = []
    for e in entities:
        created_raw = e.get("created_date") or e.get("action_builder:created_date", "")
        if created_raw and created_raw[:10] == str(target_date):
            created_today.append(e)

    count = len(created_today)
    print(f"{count} created, {len(entities)} modified total")

    if verbose and created_today:
        for e in created_today:
            given = e.get("given_name", "")
            family = e.get("family_name", "")
            entity_id = e.get("identifiers", [""])[0]
            print(f"    - {given} {family}  ({entity_id})")

    return count


def main():
    parser = argparse.ArgumentParser(description="Check AB for entities created today")
    parser.add_argument("--date", default=str(date.today()),
                        help="Date to check (YYYY-MM-DD, default: today)")
    parser.add_argument("--campaign", default=None,
                        help="Limit to one state alias (e.g. 'arizona')")
    parser.add_argument("--verbose", action="store_true",
                        help="Print individual entity names")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)

    load_dotenv(dotenv_path=".env")
    ab = ActionBuilderConnector()
    ab.connect()

    campaigns = {}
    if args.campaign:
        key = args.campaign.lower().replace(" ", "_").replace("-", "_")
        if key not in CAMPAIGN_ALIASES:
            print(f"Unknown campaign alias: {args.campaign}")
            sys.exit(1)
        campaigns[args.campaign] = CAMPAIGN_ALIASES[key]
    else:
        campaigns = {k: v for k, v in CAMPAIGN_ALIASES.items() if k != "test"}

    print(f"Checking AB for entities created on {target_date} ...")
    print()

    total = 0
    for name, cid in campaigns.items():
        total += check_campaign(ab, name, cid, target_date, args.verbose)

    print()
    print(f"Total entities created on {target_date}: {total}")


if __name__ == "__main__":
    main()
