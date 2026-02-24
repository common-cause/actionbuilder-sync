from dotenv import load_dotenv
from ccef_connections.connectors.bigquery import BigQueryConnector
load_dotenv(dotenv_path='.env')
client = BigQueryConnector(project_id='proj-tmc-mem-com')
client.connect()

# 1. Identify campaigns in updates_needed
print("=== Campaigns in updates_needed ===")
rows = list(client.query("""
    SELECT un.campaign_id, c.name, c.interact_id, c.status, COUNT(*) as update_count
    FROM `proj-tmc-mem-com.actionbuilder_sync.updates_needed` un
    JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__campaigns` c
      ON un.campaign_id = c.id
    GROUP BY 1,2,3,4
    ORDER BY update_count DESC
"""))
for r in rows:
    print(f"  id={r['campaign_id']} name={r['name']!r} interact_id={r['interact_id']} status={r['status']} updates={r['update_count']}")

# 2. Sample rows from test campaign (0e41ca37...)
print("\n=== Sample updates_needed for test campaign (0e41ca37) ===")
rows2 = list(client.query("""
    SELECT un.*
    FROM `proj-tmc-mem-com.actionbuilder_sync.updates_needed` un
    JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__campaigns` c
      ON un.campaign_id = c.id
    WHERE c.interact_id LIKE '0e41ca37%'
    LIMIT 5
"""))
if rows2:
    print("  Columns:", list(rows2[0].keys()))
    for r in rows2:
        d = dict(r)
        print(f"  entity_id={d.get('entity_id')} field_name={d.get('field_name')} change_type={d.get('change_type')}")
        print(f"    current={d.get('current_value')!r} correct={d.get('correct_value')!r}")
else:
    print("  No rows found for test campaign")

# 3. Field breakdown for test campaign
print("\n=== Field breakdown for test campaign ===")
rows3 = list(client.query("""
    SELECT un.field_name, un.change_type, COUNT(*) as cnt
    FROM `proj-tmc-mem-com.actionbuilder_sync.updates_needed` un
    JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__campaigns` c
      ON un.campaign_id = c.id
    WHERE c.interact_id LIKE '0e41ca37%'
    GROUP BY 1,2
    ORDER BY cnt DESC
"""))
for r in rows3:
    print(f"  {r['field_name']} / {r['change_type']}: {r['cnt']}")

# 4. Check if test campaign entities exist in AB data
print("\n=== Test campaign entity count in AB ===")
rows4 = list(client.query("""
    SELECT c.name, c.interact_id, COUNT(ce.entity_id) as entity_count
    FROM `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__campaigns` c
    JOIN `proj-tmc-mem-com.actionbuilder_cleaned.cln_actionbuilder__campaigns_entities` ce
      ON c.id = ce.campaign_id
    WHERE c.interact_id LIKE '0e41ca37%'
    GROUP BY 1,2
"""))
for r in rows4:
    print(f"  name={r['name']!r} interact_id={r['interact_id']} entities={r['entity_count']}")
