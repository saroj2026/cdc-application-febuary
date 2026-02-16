"""
Reprocess pipeline-5 sink from the start: write registration_date as bigint (no epochToTs),
use a new consumer group so it reads from earliest. Run fix_customer2_registration_date_postgres.sql
on Postgres first so the column is bigint.

Run from backend dir: python scripts/reprocess_pipeline5_sink.py
"""
import os
import sys
import time
import requests

KAFKA_CONNECT = os.getenv("KAFKA_CONNECT_URL", "http://72.61.233.209:8083")
SINK_CONNECTOR = "sink-pipeline-5-pg-public"
NEW_CONSUMER_GROUP = "connect-sink-pipeline-5-pg-public-v4"


def main():
    print("Ensure you ran scripts/fix_customer2_registration_date_postgres.sql on Postgres first.")
    print()
    print("1. Getting current sink connector config...")
    r = requests.get(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/config", timeout=15)
    if r.status_code != 200:
        print("   Failed to get config:", r.status_code, r.text[:300])
        sys.exit(1)
    config = r.json()
    print("   OK")

    # Remove epochToTs so we write raw bigint; Postgres column must be bigint (run fix_customer2_registration_date_postgres.sql first)
    config["transforms"] = "unwrap"
    for key in list(config.keys()):
        if key.startswith("transforms.epochToTs."):
            del config[key]
    print("   Removed epochToTs transform (sink will write registration_date as bigint)")

    # New consumer group so sink starts from earliest
    config["consumer.override.group.id"] = NEW_CONSUMER_GROUP
    config["consumer.override.auto.offset.reset"] = "earliest"
    print("   Set consumer.override.group.id =", NEW_CONSUMER_GROUP)

    print("2. Updating connector config...")
    r = requests.put(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/config", json=config, timeout=30)
    if r.status_code != 200:
        print("   Failed:", r.status_code, r.text[:300])
        sys.exit(1)
    print("   OK")
    time.sleep(2)

    print("3. Restarting connector...")
    r = requests.post(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/restart", timeout=30)
    if r.status_code not in (200, 204):
        print("   Restart returned:", r.status_code, r.text[:200])
    else:
        print("   Restart requested.")
    time.sleep(2)

    print("4. Checking status...")
    r = requests.get(f"{KAFKA_CONNECT}/connectors/{SINK_CONNECTOR}/status", timeout=15)
    if r.status_code == 200:
        d = r.json()
        conn_state = d.get("connector", {}).get("state", "?")
        task_states = [t.get("state", "?") for t in d.get("tasks", [])]
        print("   Connector:", conn_state, "| Tasks:", task_states)
    print()
    print("Done. Sink will re-consume from start (new consumer group).")
    print("Check: SELECT COUNT(*) FROM public.customer2;")
    print("Display dates: SELECT customer_id, registration_date, registration_ts FROM public.customer2 LIMIT 10;")


if __name__ == "__main__":
    main()
