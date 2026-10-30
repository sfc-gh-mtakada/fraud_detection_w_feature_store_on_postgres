"""
04_simulator.py : Payment switch + authorization service simulator.

Every tick it generates card transactions and, for each batch, sends them down BOTH paths:
  (1) Ingest REST API -> Stream FVs -> Query FRAUD_FG -> model -> decision -> Postgres OLTP
  (2) Snowpipe Streaming v2 (PIPE RAW.CARD_TRANSACTIONS_PIPE) -> RAW.CARD_TRANSACTIONS
      (feeds the CARD_PROFILE batch FV, analytics and retraining)

Fraud scenarios are injected when the app inserts into fraud.scenario_requests,
or with --scenario on the command line.

Run:  .venv/bin/python 04_simulator.py --tps 5
"""
import argparse
import os
import random
import time
from collections import deque
from datetime import datetime, timezone

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest.streaming import StreamingIngestClient  # noqa: E402

import common as c  # noqa: E402
from fraud_auth import PAT_PATH, FraudAuthorizer, new_txn, pg_connect, record_decisions  # noqa: E402

ENTRY_MODES = ["CONTACTLESS", "CHIP", "ECOM", "MAGSTRIPE"]


def load_reference(session):
    cards = session.sql(f"""
        SELECT CARD_ID, MEDIAN(AMOUNT) AS BASE, AVG(IFF(ENTRY_MODE = 'CONTACTLESS', 1, 0)) AS CTLS
        FROM {c.DATABASE}.{c.RAW_SCHEMA}.CARD_TRANSACTIONS WHERE NOT IS_FRAUD GROUP BY CARD_ID
    """).to_pandas()
    acceptors = session.table(f"{c.DATABASE}.{c.RAW_SCHEMA}.ACCEPTORS").select("ACCEPTOR_ID", "MCC").to_pandas()
    return cards.to_dict("records"), acceptors.to_dict("records")


class TxnGenerator:
    def __init__(self, cards, acceptors):
        self.cards, self.acceptors = cards, acceptors
        self.pending = deque()  # (due_epoch, txn kwargs) for scenario events

    def normal(self):
        card, acc = random.choice(self.cards), random.choice(self.acceptors)
        r = random.random()
        mode = "CONTACTLESS" if r < card["CTLS"] else random.choice(ENTRY_MODES[1:3])
        amount = max(100, round(card["BASE"] * random.lognormvariate(0, 0.5), -1))
        return new_txn(card["CARD_ID"], acc["ACCEPTOR_ID"], amount, mode, acc["MCC"])

    def inject(self, scenario, target=None):
        now = time.time()
        if scenario == "BURST":  # stolen card tapped repeatedly at several shops
            card = target or random.choice(self.cards)["CARD_ID"]
            for k in range(8):
                acc = random.choice(self.acceptors)
                self.pending.append((now + k * random.uniform(2, 5), dict(
                    card_id=card, acceptor_id=acc["ACCEPTOR_ID"], amount=round(random.uniform(3000, 9500), -1),
                    entry_mode="CONTACTLESS", mcc=acc["MCC"], scenario="BURST")))
        elif scenario == "TERMINAL":  # compromised terminal testing many cards
            acc = next((a for a in self.acceptors if a["ACCEPTOR_ID"] == target), random.choice(self.acceptors))
            for k, card in enumerate(random.sample(self.cards, 12)):
                self.pending.append((now + k * random.uniform(1, 3), dict(
                    card_id=card["CARD_ID"], acceptor_id=acc["ACCEPTOR_ID"], amount=round(random.uniform(100, 1500), -1),
                    entry_mode="CONTACTLESS", mcc=acc["MCC"], scenario="TERMINAL")))
        elif scenario == "HIGH_VALUE":  # card-not-present purchase far above usual spend
            card = next((x for x in self.cards if x["CARD_ID"] == target), random.choice(self.cards))
            acc = random.choice(self.acceptors)
            self.pending.append((now, dict(
                card_id=card["CARD_ID"], acceptor_id=acc["ACCEPTOR_ID"],
                amount=round(card["BASE"] * random.uniform(10, 20), -2), entry_mode="ECOM",
                mcc=acc["MCC"], scenario="HIGH_VALUE")))
        self.pending = deque(sorted(self.pending, key=lambda x: x[0]))

    def due(self):
        out, now = [], time.time()
        while self.pending and self.pending[0][0] <= now:
            out.append(new_txn(**self.pending.popleft()[1]))
        return out


def open_snowpipe_channel():
    client = StreamingIngestClient(
        client_name="FRAUD_SIMULATOR",
        db_name=c.DATABASE, schema_name=c.RAW_SCHEMA, pipe_name="CARD_TRANSACTIONS_PIPE",
        properties={
            "authorization_type": "PAT",
            "url": f"https://{c.SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
            "account": c.SNOWFLAKE_ACCOUNT,
            "personal_access_token": PAT_PATH.read_text().strip(),
            "role": c.PRODUCER_ROLE,
        },
    )
    channel, _ = client.open_channel("SIMULATOR_CH1")
    return client, channel


def to_raw_row(d):
    ts = datetime.strptime(d["TS"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return {"TXN_ID": d["TXN_ID"], "CARD_ID": d["CARD_ID"], "ACCEPTOR_ID": d["ACCEPTOR_ID"],
            "TS": ts.strftime("%Y-%m-%d %H:%M:%S.%f"), "AMOUNT": int(d["AMOUNT"]),
            "ENTRY_MODE": d["ENTRY_MODE"], "MCC": d["MCC"], "DEVICE_ID": None,
            "IS_FRAUD": d["SCENARIO"] != "NORMAL", "SOURCE": "STREAM"}


def poll_scenarios(pg, gen):
    with pg.cursor() as cur:
        cur.execute("""UPDATE fraud.scenario_requests SET consumed_at = now()
                       WHERE consumed_at IS NULL RETURNING scenario, target_id""")
        for scenario, target in cur.fetchall():
            print(f">>> injecting scenario {scenario} {target or ''}")
            gen.inject(scenario, target)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tps", type=float, default=2,
                    help="normal transactions per second (model is trained at ~2 TPS density)")
    ap.add_argument("--scenario", choices=["BURST", "TERMINAL", "HIGH_VALUE"])
    ap.add_argument("--duration", type=int, default=0, help="seconds to run (0 = forever)")
    args = ap.parse_args()

    auth = FraudAuthorizer()
    print(f"model {c.MODEL_NAME}/{auth.model_version}; ingest={auth.ingest_url}")
    gen = TxnGenerator(*load_reference(auth.session))
    pg = pg_connect()
    client, channel = open_snowpipe_channel()
    if args.scenario:
        gen.inject(args.scenario)

    seq, started = 0, time.time()
    try:
        while not args.duration or time.time() - started < args.duration:
            tick = time.time()
            poll_scenarios(pg, gen)
            n = int(args.tps) + (random.random() < args.tps % 1)
            txns = [gen.normal() for _ in range(n)] + gen.due()
            if txns:
                decisions = auth.authorize(txns)                          # path (1)
                record_decisions(pg, decisions)
                seq += len(decisions)
                channel.append_rows([to_raw_row(d) for d in decisions],   # path (2)
                                    end_offset_token=str(seq))
                flagged = [d for d in decisions if d["DECISION"] != "APPROVE"]
                print(f"{datetime.now():%H:%M:%S} n={len(decisions):2d} "
                      f"feature={decisions[0]['FEATURE_MS']:.0f}ms total={decisions[0]['TOTAL_MS']:.0f}ms "
                      f"flagged={[(d['SCENARIO'], d['DECISION'], round(d['SCORE'], 2)) for d in flagged]}")
            time.sleep(max(0.0, 1.0 - (time.time() - tick)))
    finally:
        channel.close()
        client.close()
        pg.close()


if __name__ == "__main__":
    main()
