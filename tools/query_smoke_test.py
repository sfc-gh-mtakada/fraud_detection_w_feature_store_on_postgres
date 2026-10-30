"""Smoke test for the Online Feature Store REST endpoints (Query API) using the FRAUD_SVC PAT."""
import json
import time
from pathlib import Path

import requests
from snowflake.ml.feature_store import online_service

import common as c

PAT = (Path.home() / ".snowflake" / "fraud_svc.pat").read_text().strip()
HEADERS = {"Authorization": f'Snowflake Token="{PAT}"', "Content-Type": "application/json"}


def main():
    fs = c.get_feature_store(c.get_session())
    query_url = online_service.endpoint_url(fs.get_online_service_status(), "query")
    body = {
        "name": c.FEATURE_GROUP, "version": c.FG_VERSION, "object_type": "feature_group",
        "metadata_options": {"include_names": True},
        "request_rows": [{"entity": {"CARD_ID": "C000001", "ACCEPTOR_ID": "A00001"},
                          "request_context": {"TXN_AMOUNT": 5000.0}}],
    }
    for _ in range(5):
        t = time.perf_counter()
        r = requests.post(f"{query_url}/api/v1/query", headers=HEADERS, json=body, timeout=10)
        print(r.status_code, f"{(time.perf_counter() - t) * 1000:.1f} ms")
    print(json.dumps(r.json(), indent=1)[:2000])


if __name__ == "__main__":
    main()
