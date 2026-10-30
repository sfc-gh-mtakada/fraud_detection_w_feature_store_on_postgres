"""
02_feature_store.py : Build the Online Feature Store for the fraud demo.

  1. Create the online service (Snowflake Postgres serving layer, size XS)
  2. Entities      : CARD (CARD_ID), ACCEPTOR (ACCEPTOR_ID)
  3. Feature views :
       CARD_PROFILE      batch, tiled 60d/90d windows   <- RAW.CARD_TRANSACTIONS (Snowpipe Streaming)
       CARD_VELOCITY     stream, continuous 5m/4h        <- Ingest API (TXN_STREAM)
       ACCEPTOR_VELOCITY stream, continuous 1h           <- Ingest API (TXN_STREAM)
       TXN_RISK          real-time, computed per request <- request + upstream FVs
  4. Feature group FRAUD_FG bundling all four for training and serving

Run locally:  .venv/bin/python 02_feature_store.py
"""
import time

import pandas as pd
from snowflake.ml.feature_store import (
    Entity,
    Feature,
    FeatureGroup,
    FeatureView,
    OnlineConfig,
    OnlineStoreType,
    RealtimeConfig,
    RequestSource,
    StreamConfig,
    StreamSource,
)
from snowflake.ml.feature_store.spec.enums import FeatureAggregationMethod
from snowflake.snowpark.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
)

import common as c

ONLINE = OnlineConfig(enable=True, target_lag="1m", store_type=OnlineStoreType.POSTGRES)
STREAM_ONLINE = OnlineConfig(enable=True, store_type=OnlineStoreType.POSTGRES)


# --------------------------------------------------------------------------- UDF logic
# Stream transform: runs at ingest time and on backfill (pandas/numpy/re/copy only).
def add_contactless_amount(df: pd.DataFrame) -> pd.DataFrame:
    df["CONTACTLESS_AMOUNT"] = df["AMOUNT"].where(df["ENTRY_MODE"] == "CONTACTLESS", 0.0)
    return df


# Real-time compute (request amount vs. card profile / velocity) lives in train/rt_features.py
# so the training job applies exactly the same logic offline.
from train.rt_features import compute_txn_risk  # noqa: E402


# --------------------------------------------------------------------------- steps
def ensure_online_service(fs):
    status = fs.get_online_service_status()
    print(f"Online service status: {status.status}")
    if status.status == "NOT_FOUND":
        print("Creating online service (size XS, billing starts now)...")
        print(fs.create_online_service(c.PRODUCER_ROLE, c.CONSUMER_ROLE, size="XS"))
        status = fs.get_online_service_status()
    while status.status != "RUNNING":
        time.sleep(30)
        status = fs.get_online_service_status()
        print(f"  status={status.status} {status.message or ''}")
    print(f"Online service RUNNING, size={status.size}")


def register_entities(fs):
    card = Entity(name="CARD", join_keys=["CARD_ID"], desc="Payment card")
    acceptor = Entity(name="ACCEPTOR", join_keys=["ACCEPTOR_ID"], desc="Merchant terminal")
    existing = {r["NAME"] for r in fs.list_entities().collect()}
    for e in (card, acceptor):
        if e.name not in existing:
            fs.register_entity(e)
    return card, acceptor


def register_card_profile(session, fs, card):
    src = session.sql(f"""
        SELECT CARD_ID, TS, AMOUNT::FLOAT AS AMOUNT,
               IFF(ENTRY_MODE = 'CONTACTLESS', 1, 0) AS IS_CONTACTLESS
        FROM {c.DATABASE}.{c.RAW_SCHEMA}.CARD_TRANSACTIONS
    """)
    fv = FeatureView(
        name="CARD_PROFILE",
        entities=[card],
        feature_df=src,
        timestamp_col="TS",
        feature_granularity="1d",
        refresh_freq="1h",
        features=[
            Feature.count("AMOUNT", "60d").alias("CARDID_COUNT_60D"),
            Feature.avg("AMOUNT", "60d").alias("CARDID_MEAN_AMOUNT_60D"),
            Feature.stddev("AMOUNT", "60d").alias("CARDID_STD_AMOUNT_60D"),
            Feature.sum("IS_CONTACTLESS", "90d").alias("CONTACTLESS_COUNT_90D"),
        ],
        online_config=ONLINE,
        desc="Long-horizon card spending profile (batch, from Snowpipe Streaming landing table)",
    )
    return fs.register_feature_view(fv, version="V1", overwrite=True)


def register_stream_fvs(session, fs, card, acceptor):
    stream = StreamSource(
        name=c.STREAM_SOURCE,
        schema=StructType([
            StructField("CARD_ID", StringType()),
            StructField("ACCEPTOR_ID", StringType()),
            StructField("TS", TimestampType(TimestampTimeZone.NTZ)),
            StructField("AMOUNT", DoubleType()),
            StructField("ENTRY_MODE", StringType()),
        ]),
        desc="Authorization requests pushed by the payment switch",
    )
    try:
        fs.register_stream_source(stream)
    except Exception as ex:  # already registered on re-run
        print(f"  stream source: {ex}")
    backfill = session.table(f"{c.DATABASE}.{c.RAW_SCHEMA}.TXN_STREAM_BACKFILL")

    def cfg():
        return StreamConfig(stream_source=stream, transformation_fn=add_contactless_amount,
                            backfill_df=backfill)

    card_vel = FeatureView(
        name="CARD_VELOCITY",
        entities=[card],
        stream_config=cfg(),
        timestamp_col="TS",
        refresh_freq="1 minute",
        feature_granularity="1 minute",
        features=[
            Feature.count("AMOUNT", "5m").alias("CARDID_COUNT_5M"),
            Feature.count("AMOUNT", "1h").alias("CARDID_COUNT_1H"),
            Feature.sum("CONTACTLESS_AMOUNT", "4h").alias("CARDID_SUM_AMOUNT_CONTACTLESS_4H"),
        ],
        online_config=STREAM_ONLINE,
        feature_aggregation_method=FeatureAggregationMethod.CONTINUOUS,
        desc="Short-window card velocity (stream)",
    )
    acc_vel = FeatureView(
        name="ACCEPTOR_VELOCITY",
        entities=[acceptor],
        stream_config=cfg(),
        timestamp_col="TS",
        refresh_freq="1 minute",
        feature_granularity="1 minute",
        features=[
            Feature.count("AMOUNT", "1h").alias("ACCEPTORID_CARDID_COUNT_1H"),
            Feature.approx_count_distinct("CARD_ID", "1h", precision=12).alias("ACCEPTORID_DISTINCT_CARDS_1H"),
        ],
        online_config=STREAM_ONLINE,
        feature_aggregation_method=FeatureAggregationMethod.CONTINUOUS,
        desc="Terminal-level velocity: card-testing signal (stream)",
    )
    return (fs.register_feature_view(card_vel, version="V1", overwrite=True),
            fs.register_feature_view(acc_vel, version="V1", overwrite=True))


def register_realtime_fv(fs, card, reg_profile, reg_card_vel):
    rt = FeatureView(
        name="TXN_RISK",
        entities=[card],
        realtime_config=RealtimeConfig(
            compute_fn=compute_txn_risk,
            sources=[
                RequestSource(schema=StructType([StructField("TXN_AMOUNT", DoubleType())])),
                reg_profile,
                reg_card_vel,
            ],
            output_schema=StructType([
                StructField("AMOUNT_TO_MEAN_RATIO", DoubleType()),
                StructField("AMOUNT_ZSCORE", DoubleType()),
                StructField("IS_BURST", DoubleType()),
            ]),
        ),
        desc="Per-request amount anomaly vs. card profile (real-time)",
    )
    return fs.register_feature_view(rt, version="V1", overwrite=True)


def register_feature_group(fs, fvs):
    fg = FeatureGroup(name=c.FEATURE_GROUP, features=list(fvs), auto_prefix=False,
                      desc="All features consumed by the fraud model")
    return fs.register_feature_group(fg, c.FG_VERSION)


def wait_online_tables(session):
    for _ in range(40):
        rows = session.sql(f"SHOW ONLINE FEATURE TABLES IN SCHEMA {c.DATABASE}.{c.FS_SCHEMA}").collect()
        states = {r["name"]: r["setup_status"] for r in rows}
        print("  ", states)
        if states and all(s == "SETUP_READY" for s in states.values()):
            return
        if any(s == "SETUP_FAILED" for s in states.values()):
            raise RuntimeError(f"Online table setup failed: {states}")
        time.sleep(15)


def get_existing(fs, name, version="V1"):
    try:
        return fs.get_feature_view(name, version)
    except Exception:
        return None


def drop_stream_stack(fs):
    """Drop FG -> real-time FV -> stream FVs (dependency order) so they can be rebuilt."""
    for drop in (lambda: fs.delete_feature_group(c.FEATURE_GROUP, c.FG_VERSION),
                 lambda: fs.delete_feature_view("TXN_RISK", "V1"),
                 lambda: fs.delete_feature_view("CARD_VELOCITY", "V1"),
                 lambda: fs.delete_feature_view("ACCEPTOR_VELOCITY", "V1")):
        try:
            drop()
        except Exception as ex:
            print(f"  skip: {str(ex)[:120]}")


def wait_backfill(session, names=("CARD_VELOCITY", "ACCEPTOR_VELOCITY")):
    """Poll STREAM_CONFIG metadata until every stream FV backfill leaves the RUNNING state."""
    for _ in range(80):
        rows = session.sql(f"""
            SELECT OBJECT_NAME, METADATA:backfill_state::STRING AS STATE
            FROM {c.DATABASE}.{c.FS_SCHEMA}._FEATURE_STORE_METADATA
            WHERE METADATA_TYPE = 'STREAM_CONFIG' AND OBJECT_NAME IN ({",".join(f"'{n}'" for n in names)})
        """).collect()
        states = {r["OBJECT_NAME"]: r["STATE"] for r in rows}
        print("   backfill:", states)
        if any(s == "FAILED" for s in states.values()):
            raise RuntimeError(f"Stream backfill failed: {states} (check TASK_HISTORY for *$BACKFILL*)")
        if len(states) == len(names) and all(s not in ("RUNNING", "PENDING", None) for s in states.values()):
            return
        time.sleep(15)
    raise TimeoutError("Stream backfill did not finish in time")


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild-stream", action="store_true",
                    help="drop and re-register stream FVs (re-runs the historical backfill)")
    args = ap.parse_args()

    session = c.get_session()
    fs = c.get_feature_store(session)
    ensure_online_service(fs)
    card, acceptor = register_entities(fs)
    reg_profile = get_existing(fs, "CARD_PROFILE") or register_card_profile(session, fs, card)
    print("CARD_PROFILE ready")
    if args.rebuild_stream:
        drop_stream_stack(fs)
    reg_card_vel, reg_acc_vel = get_existing(fs, "CARD_VELOCITY"), get_existing(fs, "ACCEPTOR_VELOCITY")
    if not (reg_card_vel and reg_acc_vel):
        reg_card_vel, reg_acc_vel = register_stream_fvs(session, fs, card, acceptor)
    wait_backfill(session)
    print("CARD_VELOCITY, ACCEPTOR_VELOCITY ready")
    reg_rt = get_existing(fs, "TXN_RISK") or register_realtime_fv(fs, card, reg_profile, reg_card_vel)
    print("TXN_RISK ready")
    wait_online_tables(session)
    try:
        fs.get_feature_group(c.FEATURE_GROUP, c.FG_VERSION)
    except Exception:
        register_feature_group(fs, [reg_profile, reg_card_vel, reg_acc_vel, reg_rt])
    print(f"Feature group {c.FEATURE_GROUP}/{c.FG_VERSION} ready")

    fg = fs.get_feature_group(c.FEATURE_GROUP, c.FG_VERSION)
    print(fs.read_feature_group(fg, keys=[["C000001", "A00001"]],
                                request_context=pd.DataFrame({"TXN_AMOUNT": [5000.0]})))


if __name__ == "__main__":
    main()
