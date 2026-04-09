"""Test note appending to Suzanne Almeida in Test campaign.
Uses the actual field structure created in the AB UI."""

from dotenv import load_dotenv
from ccef_connections.connectors.action_builder import ActionBuilderConnector

load_dotenv(dotenv_path='.env')

ab = ActionBuilderConnector()
ab.connect()

CAMPAIGN_ID = '0e41ca37-e05d-499c-943b-9d08dc8725b0'
ENTITY_ID = 'd641f0c5-9af1-47d9-b3f8-cb25ed3dbcf5'  # Suzanne Almeida

# Test 1: Event Host Notes
print("Test 1: Event Host Notes")
r1 = ab.append_note(
    campaign_id=CAMPAIGN_ID,
    entity_interact_id=ENTITY_ID,
    section='1 Million Conversations',
    field='Conversation Notes',
    name='Event Host Notes',
    note_body='Event: JP Chess, 2026-04-01. Resonant issues: Value of offbeat openings. Description: Stood around talking about the King\'s Gambit. This is test data.',
)
print(f"  Done")

# Test 2: Conversation Host Notes
print("Test 2: Conversation Host Notes")
r2 = ab.append_note(
    campaign_id=CAMPAIGN_ID,
    entity_interact_id=ENTITY_ID,
    section='1 Million Conversations',
    field='Conversation Notes',
    name='Conversation Host Notes',
    note_body='Conversation with Geff Foster, Charlotte Airport, 2026-04-01. Topic: Quality of burgers. Insight: It was a very good burger.',
)
print(f"  Done")

# Test 3: Event Attendee Notes
print("Test 3: Event Attendee Notes")
r3 = ab.append_note(
    campaign_id=CAMPAIGN_ID,
    entity_interact_id=ENTITY_ID,
    section='1 Million Conversations',
    field='Conversation Notes',
    name='Event Attendee Notes',
    note_body='Man is that guy obsessed with the Muzio Gambit. Doesn\'t he know it\'s like -3 according to stockfish?',
)
print(f"  Done")

print("\nAll 3 notes posted. Check Suzanne Almeida in the Test campaign UI.")
print("URL: https://commoncause.actionbuilder.org/entity/view/330/profile?campaignId=1&clientQueryId=null")
