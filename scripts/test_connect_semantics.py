"""
Test A — connect vs. duplicate semantics for the Organizing Team campaign plan.

Question: when the Person Signup Helper POSTs a person to campaign B whose
email already belongs to an entity in campaign A, does ActionBuilder
  (a) CONNECT the existing network-level entity to campaign B
      (same interact_id returned, new campaigns_entities row), or
  (b) CREATE a duplicate entity scoped to campaign B (new interact_id)?

Universal-section value sharing is per-entity, so answer (a) is required for
the "shared statuses across campaigns" architecture to work via the API.

Run: python scripts/test_connect_semantics.py [--execute]
Without --execute it only does the read-only "before" lookup.
"""

import json
import sys

from dotenv import load_dotenv

load_dotenv(dotenv_path='.env')

from ccef_connections.connectors.action_builder import ActionBuilderConnector  # noqa: E402

NC_CAMPAIGN = '96dca89a-61bd-49f4-87a8-4368e655f1c3'
ORG_TEAM_CAMPAIGN = '1e7e58fd-efb4-4810-91dc-2e7aac08625a'
TEST_PERSON_INTERACT_ID = '956a041d-9ea6-4d78-aa5e-4814c8512337'  # asadler@, NC only
TEST_EMAIL = 'asadler@commoncause.org'


def main() -> None:
    execute = '--execute' in sys.argv

    ab = ActionBuilderConnector()
    ab.connect()

    before = ab.get_person(NC_CAMPAIGN, TEST_PERSON_INTERACT_ID)
    print('=== BEFORE: entity as seen in North Carolina ===')
    print('identifiers:', before.get('identifiers'))
    print('name:', before.get('given_name'), before.get('family_name'))
    print('emails:', [e.get('address') for e in before.get('email_addresses', [])])

    person_data = {
        'given_name': before.get('given_name'),
        'family_name': before.get('family_name'),
        'email_addresses': [{'address': TEST_EMAIL, 'primary': True}],
    }

    if not execute:
        print('\n[dry-run] Would POST to Organizing Team:', json.dumps(person_data))
        return

    print('\n=== POSTing to Organizing Team campaign ===')
    response = ab.insert_entity(ORG_TEAM_CAMPAIGN, person_data)
    print('identifiers:', response.get('identifiers'))
    print('full response:')
    print(json.dumps(response, indent=2, default=str))

    returned_ids = [
        i.split(':', 1)[1] for i in response.get('identifiers', [])
        if i.startswith('action_builder:')
    ]
    if TEST_PERSON_INTERACT_ID in returned_ids:
        print('\nRESULT: CONNECT — same entity now linked to both campaigns.')
    else:
        print('\nRESULT: DUPLICATE — a new entity was created:', returned_ids)


if __name__ == '__main__':
    main()
