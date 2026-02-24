from dotenv import load_dotenv
from ccef_connections.connectors.bigquery import BigQueryConnector
load_dotenv(dotenv_path='.env')
client = BigQueryConnector(project_id='proj-tmc-mem-com')
client.connect()

print("=== test_campaign_update_summary ===")
rows = list(client.query("SELECT * FROM `proj-tmc-mem-com.actionbuilder_sync.test_campaign_update_summary` ORDER BY field_name, change_type"))
for r in rows:
    d = dict(r)
    delta = f"avg_delta={d['avg_abs_delta']:.2f}" if d['avg_abs_delta'] is not None else "avg_delta=N/A"
    print(f"  {d['field_name']} / {d['change_type']}: {d['entity_count']} entities  current=[{d['current_min']}, {d['current_max']}]  correct=[{d['correct_min']}, {d['correct_max']}]  {delta}")

print("\n=== test_campaign_updates sample (5 rows) ===")
rows2 = list(client.query("SELECT entity_id, field_name, change_type, current_value, correct_value, first_name, last_name, primary_email FROM `proj-tmc-mem-com.actionbuilder_sync.test_campaign_updates` LIMIT 5"))
for r in rows2:
    d = dict(r)
    print(f"  {d.get('first_name','')} {d.get('last_name','')} <{d.get('primary_email','')}>  field={d['field_name']}  {d['current_value']} -> {d['correct_value']}  ({d['change_type']})")

print("\n=== test_campaign_updates total row count ===")
rows3 = list(client.query("SELECT COUNT(*) as cnt FROM `proj-tmc-mem-com.actionbuilder_sync.test_campaign_updates`"))
print(f"  {rows3[0]['cnt']} rows")
