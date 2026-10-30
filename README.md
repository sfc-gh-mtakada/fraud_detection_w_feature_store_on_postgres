# Real-Time Fraud Detection on Snowflake

A runnable demo of the *Financial Services: Real-Time Fraud Detection* pattern. It is built on:

- the Snowflake **Online Feature Store**: batch, stream and real-time feature views, plus a feature group
- **Snowpipe Streaming** (high-performance architecture)
- **Container Runtime** ML Jobs and the **Model Registry**
- **Snowflake Postgres** for the OLTP decision log and the analyst case queue

All data is synthetic: 5,000 cards, 800 terminals, about 2.5M card transactions in JPY.

## Architecture

![Architecture](images/architecture.png)

The diagram labels are in Japanese. 経路1 判定 = Path 1: decision, 経路2 データ基盤 = Path 2: data platform, 決済スイッチ役 = payment switch. Solid lines are data flows; dotted lines are feature reads.

**Why there are two ingestion paths.** The documented way to get online data into a stream feature view is the Feature Store **Ingest REST API**; Snowpipe Streaming isn't a documented source for stream FVs. Each path has its own job:

- **Ingest API:** freshest features for the authorization decision.
- **Snowpipe Streaming:** the complete raw transaction record. It feeds the batch FV, analytics and retraining.

## Demo

![Demo](images/postgres-demo.gif)

## Feature mapping

| Feature view | Type | Features |
|---|---|---|
| `CARD_PROFILE/V1` | Batch, tiled 1d, `refresh_freq=1h` | CARDID_COUNT_60D, CARDID_MEAN_AMOUNT_60D, CARDID_STD_AMOUNT_60D, CONTACTLESS_COUNT_90D |
| `CARD_VELOCITY/V1` | Stream, continuous aggregation | CARDID_COUNT_5M, CARDID_COUNT_1H, CARDID_SUM_AMOUNT_CONTACTLESS_4H |
| `ACCEPTOR_VELOCITY/V1` | Stream, continuous aggregation | ACCEPTORID_CARDID_COUNT_1H, ACCEPTORID_DISTINCT_CARDS_1H (`approx_count_distinct`) |
| `TXN_RISK/V1` | Real-time (`compute_fn`) | AMOUNT_TO_MEAN_RATIO, AMOUNT_ZSCORE, IS_BURST |
| `FRAUD_FG/V1` | Feature group | all of the above, served by one Query API call |

## Files

| File | Purpose |
|---|---|
| `00_setup.sql` | Roles, warehouse, compute pool, database, raw tables, Snowpipe Streaming PIPE, grants |
| `00_postgres_oltp.sql` | Postgres OLTP schema: `fraud.auth_decisions`, `fraud.cases`, `fraud.scenario_requests` |
| `01_generate_history.sql` | Synthetic 90-day history, a dense 3-day window at ~2 TPS, and 3 fraud patterns |
| `02_feature_store.py` | Online service (XS), entities, 4 feature views, feature group |
| `03_train.py`, `train/` | ML Job on Container Runtime: point-in-time training set, XGBoost, Model Registry, thresholds |
| `fraud_auth.py` | Authorization path: Query API, scoring, Postgres write, then Ingest API |
| `04_simulator.py` | Generates traffic, executes path (1), and streams path (2) through Snowpipe Streaming |
| `05_app/app.py` | Streamlit console |
| `99_teardown.py` | Stops billing (`--all` drops everything) |
| `tools/` | PAT creation, Postgres schema apply, Query API smoke test |

## Setup

Prerequisites:

- An AWS or Azure account region. GCP isn't supported by the Online Feature Store.
- ACCOUNTADMIN.
- Python 3.12.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

Scripts that connect with the PAT (the simulator, the app, and runs with `FRAUD_USE_PAT=1`) read the account identifier from the `SNOWFLAKE_ACCOUNT` environment variable.

```bash
export SNOWFLAKE_ACCOUNT=<orgname>-<account_name>
```

1. Run `00_setup.sql` in a worksheet.
2. Create the service user `FRAUD_SVC` with a network policy that allows your client IP. A PAT only authenticates if its user is subject to a network policy. Then generate its PAT into `~/.snowflake/fraud_svc.pat`:
   `.venv/bin/python tools/create_pat.py`
3. Create the Postgres instance `FRAUD_OLTP`, for example `BURST_S`, 10GB. `BURST_S` is available on AWS only; on Azure use `BURST_M` or larger. Attach a network policy whose network rule uses `MODE = POSTGRES_INGRESS`. Save its connection as service `fraud_oltp` in `~/.pg_service.conf` and `~/.pgpass`. Then apply the schema:
   `.venv/bin/python tools/apply_pg_sql.py`
4. Run `01_generate_history.sql`.
5. Build the feature store. **The online service starts billing here.**
   `FRAUD_USE_PAT=1 .venv/bin/python 02_feature_store.py`
6. Train and register the model:
   `FRAUD_USE_PAT=1 .venv/bin/python 03_train.py`
7. Run the simulator and the console:
   `.venv/bin/python 04_simulator.py` and `.venv/bin/streamlit run 05_app/app.py`

## Demo script

1. **Live monitor.**
   - Normal traffic at about 2 TPS is almost entirely APPROVE.
   - Point out the feature-fetch latency for the whole feature group: one REST call that combines batch, stream and real-time features.
2. **Inject "Card burst".**
   - The sidebar button writes to Postgres, and the simulator picks it up.
   - `CARDID_COUNT_5M` climbs with each tap.
   - The first 2–3 taps pass, then the card is declined, typically from the 4th tap.
   - This works because stream FV values are typically updated within ~200ms (P50) of each ingest.
3. **Inject "Compromised terminal".**
   - `ACCEPTORID_DISTINCT_CARDS_1H` rises on one terminal, and its later cards are declined.
4. **Authorize a transaction.**
   - Try a 48,000 JPY contactless payment on a card that normally spends about 2,000–5,000 JPY.
   - It is declined with `AMOUNT_TO_MEAN_RATIO` (real-time FV) as the top SHAP reason.
5. **Case queue.** An analyst confirms or dismisses cases in Postgres (OLTP).
6. **Data platform.**
   - The same events land in `RAW.CARD_TRANSACTIONS` through Snowpipe Streaming within seconds.
   - From there they feed the batch FV and retraining.
   - Contrast the warehouse query time with the online fetch time.

## Measured in this environment

These numbers come from a laptop in Tokyo calling AWS ap-northeast-1 through a corporate network, with an XS online service.

- Online feature fetch for `FRAUD_FG`, client side: p50 about 25–30ms per call.
- Authorization (features, scoring and decision), client side: p50 about 60ms.
- Snowpipe Streaming landing lag: about 6s.
- Model (validation set): ROC AUC 0.976, PR AUC 0.785.
  - The REVIEW threshold is set at 1% false-positive rate (FPR) and the DECLINE threshold at 0.3% FPR.
  - In a 150-second live run, 1 of 300 normal transactions was flagged.

Client-side latency includes network distance. The Query API can return server-side timings (`include_diagnostics`), which appear in the app's Authorize tab.

## Notes and caveats

- **Container Runtime 3.0** was released on 2026-10-27 with a staged rollout.
  - `03_train.py` requests `runtime_environment="3.0"`.
  - If the image isn't available in the account yet, it falls back to the default runtime and pip-installs `snowflake-ml-python>=2.5` through `PYPI_ACCESS_INTEGRATION`.
- **Real-time FV in training.**
  - In this environment, `generate_training_set` with the `TXN_RISK` real-time FV failed with `invalid identifier 'TS'`.
  - The training job therefore joins the three materialized FVs and applies the *same* `compute_txn_risk` function (`train/rt_features.py`) in pandas, so the online and offline logic stay identical.
- **Training–serving parity for stream features.**
  - In this environment, the offline point-in-time training set did not include the event at the spine timestamp itself (normal-traffic `CARDID_COUNT_5M` averaged 0.02 offline).
  - The authorization path therefore reads features *before* ingesting the current event, so online values match what the model was trained on.
- **Traffic density.**
  - Velocity features depend on events per card or terminal per hour, and the model is trained on the dense 3-day window generated at about 2 TPS.
  - Running the simulator much faster (`--tps`) inflates velocity features and creates false positives.
- **Stream FV backfill** runs as tasks owned by the producer role. That role needs `EXECUTE TASK` and `EXECUTE MANAGED TASK`; without them the backfill fails silently and the offline stream features are all zeros.
- **Network policy.** The REST endpoints and Postgres see your *egress* IP. Behind a corporate proxy or VPN this can differ from the IP you expect; the error message shows the IP to allow.

## Cost and cleanup

The online service and the Postgres instance bill continuously while they exist.

- `.venv/bin/python 99_teardown.py`: drops the online service and suspends Postgres and the compute pool. Suspending stops compute; storage may still be billed.
- `.venv/bin/python 99_teardown.py --all`: removes every demo object.
