"""
05_app/app.py : Real-Time Fraud Detection console.

  Live monitor   : decisions streamed into Snowflake Postgres (OLTP) by the simulator
  Authorize      : run one transaction through Query API -> model, with feature contributions
  Case queue     : analyst workflow on Postgres (REVIEW / DECLINE cases)
  Data platform  : Snowpipe Streaming landing table + Feature Store objects in Snowflake

Run:  .venv/bin/streamlit run 05_app/app.py
"""
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st
import xgboost as xgb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as c  # noqa: E402
from fraud_auth import MODEL_FEATURES, FraudAuthorizer, new_txn, pg_connect, record_decisions  # noqa: E402

st.set_page_config(page_title="Real-Time Fraud Detection", layout="wide")

DECISION_COLORS = {"APPROVE": "#2e7d32", "REVIEW": "#ef6c00", "DECLINE": "#c62828"}
FEATURE_SOURCE = {
    "CARDID_COUNT_60D": "Batch", "CARDID_MEAN_AMOUNT_60D": "Batch", "CARDID_STD_AMOUNT_60D": "Batch",
    "CONTACTLESS_COUNT_90D": "Batch", "CARDID_COUNT_5M": "Stream", "CARDID_COUNT_1H": "Stream",
    "CARDID_SUM_AMOUNT_CONTACTLESS_4H": "Stream", "ACCEPTORID_CARDID_COUNT_1H": "Stream",
    "ACCEPTORID_DISTINCT_CARDS_1H": "Stream", "AMOUNT_TO_MEAN_RATIO": "Real-time",
    "AMOUNT_ZSCORE": "Real-time", "IS_BURST": "Real-time", "TXN_AMOUNT": "Request",
}


@st.cache_resource(show_spinner="Connecting to Snowflake, Online Feature Store and Postgres...")
def resources():
    auth = FraudAuthorizer()
    return auth, pg_connect()


def pg_df(sql, params=None):
    _, pg = resources()
    with pg.cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def style_decision(v):
    return f"color: white; background-color: {DECISION_COLORS.get(v, '#555')}"


auth, pg = resources()

# ------------------------------------------------------------------ sidebar
with st.sidebar:
    st.header("Real-Time Fraud Detection")
    st.caption("Snowflake Online Feature Store + Snowpipe Streaming + Snowflake Postgres")
    st.markdown(f"**Model** `{c.MODEL_NAME}/{auth.model_version}`  \n"
                f"REVIEW ≥ {auth.review_threshold:.2f} / DECLINE ≥ {auth.decline_threshold:.2f}")
    st.subheader("Inject an attack")
    st.caption("Picked up by 04_simulator.py within ~1s")
    for scenario, label in [("BURST", "Card burst (8 taps in ~30s)"),
                            ("TERMINAL", "Compromised terminal (12 cards)"),
                            ("HIGH_VALUE", "High-value e-commerce")]:
        if st.button(label, use_container_width=True):
            with pg.cursor() as cur:
                cur.execute("INSERT INTO fraud.scenario_requests (scenario) VALUES (%s)", (scenario,))
            st.toast(f"{scenario} queued")

tab_live, tab_auth, tab_cases, tab_platform = st.tabs(
    ["Live monitor", "Authorize a transaction", "Case queue", "Data platform"])

# ------------------------------------------------------------------ live monitor
with tab_live:
    @st.fragment(run_every=2)
    def live():
        kpi = pg_df("""
            SELECT count(*) AS n,
                   count(*) FILTER (WHERE decision = 'DECLINE') AS declined,
                   count(*) FILTER (WHERE decision = 'REVIEW')  AS review,
                   percentile_cont(0.5)  WITHIN GROUP (ORDER BY feature_ms) AS p50,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY feature_ms) AS p95,
                   percentile_cont(0.5)  WITHIN GROUP (ORDER BY total_ms)   AS total_p50
            FROM fraud.auth_decisions WHERE created_at > now() - interval '5 minutes'""").iloc[0]
        k = st.columns(5)
        k[0].metric("Txns (5 min)", int(kpi.n))
        k[1].metric("Declined", int(kpi.declined))
        k[2].metric("Review", int(kpi.review))
        k[3].metric("Feature fetch p50 / p95", f"{(kpi.p50 or 0):.0f} / {(kpi.p95 or 0):.0f} ms")
        k[4].metric("Authorization p50", f"{(kpi.total_p50 or 0):.0f} ms")

        recent = pg_df("""
            SELECT to_char(ts AT TIME ZONE 'Asia/Tokyo', 'HH24:MI:SS') AS time, card_id, acceptor_id,
                   amount, entry_mode, round(score::numeric, 3) AS score, decision, scenario,
                   (features->>'CARDID_COUNT_5M')::float AS cnt_5m,
                   (features->>'ACCEPTORID_DISTINCT_CARDS_1H')::float AS acc_cards_1h,
                   round((features->>'AMOUNT_TO_MEAN_RATIO')::numeric, 2) AS amt_ratio,
                   round(feature_ms::numeric, 1) AS feature_ms
            FROM fraud.auth_decisions ORDER BY created_at DESC LIMIT 25""")
        left, right = st.columns([3, 2])
        with left:
            st.subheader("Latest authorizations")
            st.dataframe(recent.style.map(style_decision, subset=["decision"]),
                         hide_index=True, use_container_width=True, height=560)
        with right:
            st.subheader("Flagged in the last 5 minutes")
            flagged = pg_df("""
                SELECT to_char(ts AT TIME ZONE 'Asia/Tokyo', 'HH24:MI:SS') AS time, scenario, decision,
                       card_id, acceptor_id, round(score::numeric, 2) AS score
                FROM fraud.auth_decisions
                WHERE decision <> 'APPROVE' AND created_at > now() - interval '5 minutes'
                ORDER BY created_at DESC LIMIT 15""")
            st.dataframe(flagged.style.map(style_decision, subset=["decision"]),
                         hide_index=True, use_container_width=True)
            lat = pg_df("""
                SELECT date_trunc('second', created_at) AS t, avg(feature_ms) AS feature_ms, avg(total_ms) AS total_ms
                FROM fraud.auth_decisions WHERE created_at > now() - interval '3 minutes'
                GROUP BY 1 ORDER BY 1""")
            if not lat.empty:
                st.caption("Latency per second (ms)")
                st.line_chart(lat.set_index("t"), height=220)
    live()

# ------------------------------------------------------------------ single authorization
with tab_auth:
    st.subheader("Authorize one transaction end-to-end")
    st.caption("Query API (FRAUD_FG: batch + stream + real-time) -> XGBoost from Model Registry -> "
               "Postgres OLTP -> Ingest API")
    with st.form("auth"):
        f = st.columns(4)
        card_id = f[0].text_input("Card", "C000123")
        acceptor_id = f[1].text_input("Acceptor (terminal)", "A00042")
        amount = f[2].number_input("Amount (JPY)", min_value=100, value=48000, step=1000)
        mode = f[3].selectbox("Entry mode", ["CONTACTLESS", "CHIP", "ECOM", "MAGSTRIPE"])
        submitted = st.form_submit_button("Authorize", type="primary")
    if submitted:
        d = auth.authorize([new_txn(card_id, acceptor_id, amount, mode, scenario="MANUAL")])[0]
        record_decisions(pg, [d])
        r = st.columns(4)
        r[0].markdown(f"<h2 style='color:{DECISION_COLORS[d['DECISION']]}'>{d['DECISION']}</h2>",
                      unsafe_allow_html=True)
        r[1].metric("Fraud score", f"{d['SCORE']:.3f}")
        r[2].metric("Online feature fetch", f"{d['FEATURE_MS']:.1f} ms")
        r[3].metric("Feature fetch + scoring", f"{d['TOTAL_MS']:.1f} ms")

        # Per-feature contribution (SHAP values from XGBoost) to explain the decision
        X = pd.DataFrame([d["FEATURES"]]).reindex(columns=MODEL_FEATURES).astype(float).fillna(0.0)
        contrib = auth.model.get_booster().predict(xgb.DMatrix(X), pred_contribs=True)[0][:-1]
        expl = pd.DataFrame({"feature": MODEL_FEATURES, "source": [FEATURE_SOURCE[n] for n in MODEL_FEATURES],
                             "value": X.iloc[0].values, "contribution": contrib}) \
            .sort_values("contribution", key=abs, ascending=False)
        a, b = st.columns([3, 2])
        a.dataframe(expl.style.format({"value": "{:,.2f}", "contribution": "{:+.3f}"})
                    .background_gradient(subset=["contribution"], cmap="RdYlGn_r"),
                    hide_index=True, use_container_width=True)
        b.bar_chart(expl.head(8).set_index("feature")["contribution"], horizontal=True)
        if d.get("DIAGNOSTICS"):
            with st.expander("Server-side diagnostics (Query API)"):
                st.json(d["DIAGNOSTICS"])

# ------------------------------------------------------------------ case queue
with tab_cases:
    st.subheader("Open cases (Snowflake Postgres)")
    cases = pg_df("""
        SELECT c.case_id, to_char(d.ts AT TIME ZONE 'Asia/Tokyo', 'MM-DD HH24:MI:SS') AS time, d.decision,
               d.scenario, d.card_id, d.acceptor_id, d.amount, d.entry_mode, round(d.score::numeric, 3) AS score
        FROM fraud.cases c JOIN fraud.auth_decisions d USING (txn_id)
        WHERE c.status = 'OPEN' ORDER BY d.score DESC LIMIT 50""")
    st.dataframe(cases.style.map(style_decision, subset=["decision"]), hide_index=True, use_container_width=True)
    if not cases.empty:
        cols = st.columns([2, 1, 1])
        case_id = cols[0].selectbox("Case", cases["case_id"])
        for col, status in ((cols[1], "CONFIRMED_FRAUD"), (cols[2], "FALSE_POSITIVE")):
            if col.button(status.replace("_", " ").title(), use_container_width=True):
                with pg.cursor() as cur:
                    cur.execute("UPDATE fraud.cases SET status = %s, analyst = current_user, updated_at = now() "
                                "WHERE case_id = %s", (status, int(case_id)))
                st.rerun()

# ------------------------------------------------------------------ data platform
with tab_platform:
    st.subheader("Snowpipe Streaming v2 -> RAW.CARD_TRANSACTIONS")

    @st.fragment(run_every=5)
    def platform():
        t0 = time.perf_counter()
        land = auth.session.sql(f"""
            SELECT SOURCE, COUNT(*) AS ROWS_, MAX(TS) AS LAST_EVENT_UTC,
                   DATEDIFF('second', MAX(TS), SYSDATE()) AS SECONDS_BEHIND
            FROM {c.DATABASE}.{c.RAW_SCHEMA}.CARD_TRANSACTIONS GROUP BY 1 ORDER BY 1""").to_pandas()
        st.dataframe(land, hide_index=True, use_container_width=True)
        st.caption(f"Warehouse query: {(time.perf_counter() - t0) * 1000:.0f} ms "
                   "(compare with the online feature fetch latency above)")
    platform()

    st.subheader("Feature Store")
    st.markdown("""
| Feature view | Type | Source | Features |
|---|---|---|---|
| `CARD_PROFILE/V1` | Batch (tiled 1d) | `RAW.CARD_TRANSACTIONS` via Snowpipe Streaming | CARDID_COUNT_60D, CARDID_MEAN/STD_AMOUNT_60D, CONTACTLESS_COUNT_90D |
| `CARD_VELOCITY/V1` | Stream (continuous) | Ingest API `TXN_STREAM` | CARDID_COUNT_5M, CARDID_COUNT_1H, CARDID_SUM_AMOUNT_CONTACTLESS_4H |
| `ACCEPTOR_VELOCITY/V1` | Stream (continuous) | Ingest API `TXN_STREAM` | ACCEPTORID_CARDID_COUNT_1H, ACCEPTORID_DISTINCT_CARDS_1H |
| `TXN_RISK/V1` | Real-time | Request + upstream FVs | AMOUNT_TO_MEAN_RATIO, AMOUNT_ZSCORE, IS_BURST |
| `FRAUD_FG/V1` | Feature group | all of the above | one Query API call per authorization |
""")
