"""
train/train_job.py : Training job executed on Container Runtime 3.0 (ML Job).

  spine (labelled txns) --generate_training_set(batch + stream FVs)--> point-in-time features
  + TXN_RISK compute_fn (shared module) -> XGBoost -> Model Registry (FRAUD_DEMO.ML.FRAUD_XGB)
"""
import json

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from snowflake.ml.feature_store import FeatureStore
from snowflake.ml.registry import Registry
from snowflake.snowpark import Session

from rt_features import compute_txn_risk  # same logic as the TXN_RISK real-time FV

DB, FS_SCHEMA, ML_SCHEMA, WH = "FRAUD_DEMO", "FS", "ML", "FRAUD_WH"
FEATURES = [
    "CARDID_COUNT_60D", "CARDID_MEAN_AMOUNT_60D", "CARDID_STD_AMOUNT_60D", "CONTACTLESS_COUNT_90D",
    "CARDID_COUNT_5M", "CARDID_COUNT_1H", "CARDID_SUM_AMOUNT_CONTACTLESS_4H",
    "ACCEPTORID_CARDID_COUNT_1H", "ACCEPTORID_DISTINCT_CARDS_1H",
    "AMOUNT_TO_MEAN_RATIO", "AMOUNT_ZSCORE", "IS_BURST", "TXN_AMOUNT",
]


def main():
    session = Session.builder.getOrCreate()
    session.use_role("FRAUD_FS_PRODUCER")
    session.use_warehouse(WH)
    session.sql("ALTER SESSION SET TIMEZONE = 'UTC'").collect()  # required for offline reads
    fs = FeatureStore(session, DB, FS_SCHEMA, default_warehouse=WH)

    # Train on the dense recent window: same traffic density as the live simulator (~2 TPS),
    # so per-card / per-terminal velocity features mean the same thing offline and online.
    spine = session.sql(f"""
        WITH t AS (
          SELECT CARD_ID, ACCEPTOR_ID, TS AS QUERY_TS, AMOUNT::FLOAT AS TXN_AMOUNT, IS_FRAUD::INT AS LABEL
          FROM {DB}.RAW.CARD_TRANSACTIONS
          WHERE SOURCE = 'HISTORY_DENSE'
        )
        SELECT * FROM t WHERE LABEL = 1
        UNION ALL
        SELECT * FROM (SELECT * FROM t WHERE LABEL = 0) SAMPLE (150000 ROWS)
    """)
    # Point-in-time join of the materialized FVs (batch + stream). The real-time FV (TXN_RISK)
    # is recomputed below with the identical compute_fn, so online/offline logic cannot drift.
    fvs = [fs.get_feature_view(n, "V1") for n in ("CARD_PROFILE", "CARD_VELOCITY", "ACCEPTOR_VELOCITY")]
    train_df = fs.generate_training_set(
        spine_df=spine,
        features=fvs,
        spine_timestamp_col="QUERY_TS",
        spine_label_cols=["LABEL"],
        join_method="cte",
    ).to_pandas()
    train_df = train_df.reset_index(drop=True)
    rt = compute_txn_risk(train_df[["TXN_AMOUNT"]], train_df, train_df)
    train_df = pd.concat([train_df, rt], axis=1)
    print(f"training rows={len(train_df)}, fraud={int(train_df['LABEL'].sum())}")

    X = train_df[FEATURES].astype(float).fillna(0.0)
    y = train_df["LABEL"].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

    model = xgb.XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.08, subsample=0.9, colsample_bytree=0.9,
        scale_pos_weight=float((y_tr == 0).sum() / max((y_tr == 1).sum(), 1)),
        eval_metric="aucpr", tree_method="hist",
    )
    model.fit(X_tr, y_tr)
    proba = model.predict_proba(X_te)[:, 1]
    neg = proba[y_te.values == 0]
    pos = proba[y_te.values == 1]
    # Operating points chosen on validation data: REVIEW at ~1% FPR, DECLINE at ~0.3% FPR
    review_t, decline_t = float(np.quantile(neg, 0.99)), float(np.quantile(neg, 0.997))
    metrics = {"roc_auc": float(roc_auc_score(y_te, proba)),
               "pr_auc": float(average_precision_score(y_te, proba)),
               "review_threshold": review_t, "decline_threshold": decline_t,
               "recall_at_review": float((pos >= review_t).mean()),
               "recall_at_decline": float((pos >= decline_t).mean())}
    print(json.dumps(metrics))

    reg = Registry(session=session, database_name=DB, schema_name=ML_SCHEMA)
    mv = reg.log_model(
        model,
        model_name="FRAUD_XGB",
        version_name=f"V{pd.Timestamp.utcnow():%Y%m%d%H%M}",
        sample_input_data=X_tr.head(50),
        metrics=metrics,
        target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"],
        comment="Fraud classifier trained on FRAUD_FG/V1",
    )
    reg.get_model("FRAUD_XGB").default = mv.version_name
    print(f"registered FRAUD_XGB/{mv.version_name}")


if __name__ == "__main__":
    main()
