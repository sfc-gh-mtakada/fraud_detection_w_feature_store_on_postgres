"""
fraud_auth.py : Authorization path shared by the simulator and the Streamlit app.

  ingest(txns)  -> Ingest REST API  (updates CARD_VELOCITY / ACCEPTOR_VELOCITY in <200ms)
  features(txns)-> Query REST API   (FRAUD_FG: batch + stream + real-time in one call)
  score()       -> XGBoost from the Snowflake Model Registry (loaded once, scored in-process)
  record()      -> Snowflake Postgres OLTP (fraud.auth_decisions / fraud.cases)
"""
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from snowflake.ml.feature_store import online_service
from snowflake.ml.registry import Registry

import common as c

PG_SERVICE = "fraud_oltp"
PAT_PATH = Path.home() / ".snowflake" / "fraud_svc.pat"
MODEL_FEATURES = [
    "CARDID_COUNT_60D", "CARDID_MEAN_AMOUNT_60D", "CARDID_STD_AMOUNT_60D", "CONTACTLESS_COUNT_90D",
    "CARDID_COUNT_5M", "CARDID_COUNT_1H", "CARDID_SUM_AMOUNT_CONTACTLESS_4H",
    "ACCEPTORID_CARDID_COUNT_1H", "ACCEPTORID_DISTINCT_CARDS_1H",
    "AMOUNT_TO_MEAN_RATIO", "AMOUNT_ZSCORE", "IS_BURST", "TXN_AMOUNT",
]
MAX_INGEST = 50   # Ingest API: records per source per request
MAX_QUERY = 10    # Query API: entities per request


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def new_txn(card_id, acceptor_id, amount, entry_mode, mcc="5999", scenario="NORMAL"):
    return {"TXN_ID": str(uuid.uuid4()), "CARD_ID": card_id, "ACCEPTOR_ID": acceptor_id,
            "TS": utc_now_iso(), "AMOUNT": float(amount), "ENTRY_MODE": entry_mode,
            "MCC": mcc, "SCENARIO": scenario}


class FraudAuthorizer:
    def __init__(self, session=None, load_model=True):
        self.session = session or c.get_session(use_pat=True)
        fs = c.get_feature_store(self.session)
        status = fs.get_online_service_status()
        self.ingest_url = online_service.endpoint_url(status, "ingest") + "/api/v1/ingest"
        self.query_url = online_service.endpoint_url(status, "query") + "/api/v1/query"
        pat = PAT_PATH.read_text().strip()
        self.http = requests.Session()
        self.http.headers.update({"Authorization": f'Snowflake Token="{pat}"',
                                  "Content-Type": "application/json"})
        self.feature_names = None
        self.model = None
        self.model_version = None
        self.review_threshold, self.decline_threshold = c.REVIEW_THRESHOLD, c.DECLINE_THRESHOLD
        if load_model:
            mv = Registry(self.session, database_name=c.DATABASE, schema_name=c.ML_SCHEMA) \
                .get_model(c.MODEL_NAME).default
            self.model = mv.load(force=True)
            self.model_version = mv.version_name
            # Use the operating points selected at training time when available
            m = mv.show_metrics()
            self.review_threshold = float(m.get("review_threshold", self.review_threshold))
            self.decline_threshold = float(m.get("decline_threshold", self.decline_threshold))

    # ------------------------------------------------------------------ REST calls
    def ingest(self, txns):
        """Push events to the stream source. Returns client-side latency (ms) of the last call."""
        ms = 0.0
        for i in range(0, len(txns), MAX_INGEST):
            records = [{k: t[k] for k in ("CARD_ID", "ACCEPTOR_ID", "TS", "AMOUNT", "ENTRY_MODE")}
                       for t in txns[i:i + MAX_INGEST]]
            t0 = time.perf_counter()
            r = self.http.post(self.ingest_url, json={"records": {c.STREAM_SOURCE: records}}, timeout=10)
            ms = (time.perf_counter() - t0) * 1000
            if r.status_code not in (200, 207):
                raise RuntimeError(f"ingest failed HTTP {r.status_code}: {r.text[:300]}")
        return ms

    def features(self, txns):
        """Fetch FRAUD_FG features for each txn. Returns (DataFrame, per-call latency ms, server diagnostics)."""
        rows, lat, diag = [], [], None
        for i in range(0, len(txns), MAX_QUERY):
            chunk = txns[i:i + MAX_QUERY]
            body = {
                "name": c.FEATURE_GROUP, "version": c.FG_VERSION, "object_type": "feature_group",
                "include_diagnostics": True,
                "request_rows": [{"entity": {"CARD_ID": t["CARD_ID"], "ACCEPTOR_ID": t["ACCEPTOR_ID"]},
                                  "request_context": {"TXN_AMOUNT": t["AMOUNT"]}} for t in chunk],
            }
            if self.feature_names is None:
                body["metadata_options"] = {"include_names": True}
            t0 = time.perf_counter()
            r = self.http.post(self.query_url, json=body, timeout=10)
            ms = (time.perf_counter() - t0) * 1000
            if r.status_code not in (200, 207):
                raise RuntimeError(f"query failed HTTP {r.status_code}: {r.text[:300]}")
            payload = r.json()
            if self.feature_names is None:
                self.feature_names = [m["name"] for m in payload["metadata"]["features"]]
            diag = payload.get("diagnostics", diag)
            rows += [res["features"] for res in payload["results"]]
            lat += [ms] * len(chunk)
        df = pd.DataFrame(rows, columns=self.feature_names)
        df["TXN_AMOUNT"] = [t["AMOUNT"] for t in txns]
        return df, lat, diag

    # ------------------------------------------------------------------ scoring
    def score(self, feats: pd.DataFrame) -> np.ndarray:
        X = feats.reindex(columns=MODEL_FEATURES).astype(float).fillna(0.0)
        return self.model.predict_proba(X)[:, 1]

    def decide(self, score: float) -> str:
        if score >= self.decline_threshold:
            return "DECLINE"
        return "REVIEW" if score >= self.review_threshold else "APPROVE"

    def authorize(self, txns):
        """Full path: features -> score -> decision -> ingest. Returns list of decision dicts.

        Features are read BEFORE the current event is ingested, matching the offline
        point-in-time join used for training (which only sees strictly earlier events).
        """
        t0 = time.perf_counter()
        feats, feat_ms, diag = self.features(txns)
        scores = self.score(feats)
        total_ms = (time.perf_counter() - t0) * 1000
        ingest_ms = self.ingest(txns)
        out = []
        for i, t in enumerate(txns):
            fvec = {k: (None if pd.isna(v) else float(v)) for k, v in feats.iloc[i].items()}
            out.append({**t, "SCORE": float(scores[i]), "DECISION": self.decide(float(scores[i])),
                        "FEATURES": fvec, "INGEST_MS": ingest_ms, "FEATURE_MS": feat_ms[i],
                        "TOTAL_MS": total_ms, "DIAGNOSTICS": diag})
        return out


# ---------------------------------------------------------------------- Postgres OLTP
def pg_connect():
    conn = psycopg2.connect(service=PG_SERVICE)
    conn.autocommit = True
    return conn


def record_decisions(conn, decisions):
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO fraud.auth_decisions
              (txn_id, card_id, acceptor_id, ts, amount, entry_mode, scenario, score, decision,
               features, feature_ms, total_ms)
            VALUES %s ON CONFLICT (txn_id) DO NOTHING""",
            [(d["TXN_ID"], d["CARD_ID"], d["ACCEPTOR_ID"], d["TS"], d["AMOUNT"], d["ENTRY_MODE"],
              d["SCENARIO"], d["SCORE"], d["DECISION"], json.dumps(d["FEATURES"]),
              d["FEATURE_MS"], d["TOTAL_MS"]) for d in decisions])
        flagged = [(d["TXN_ID"],) for d in decisions if d["DECISION"] != "APPROVE"]
        if flagged:
            psycopg2.extras.execute_values(cur, "INSERT INTO fraud.cases (txn_id) VALUES %s", flagged)
