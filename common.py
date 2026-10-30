"""Shared settings and helpers for the fraud detection demo."""
import os

from snowflake.snowpark import Session

DATABASE = "FRAUD_DEMO"
FS_SCHEMA = "FS"
RAW_SCHEMA = "RAW"
ML_SCHEMA = "ML"
WAREHOUSE = "FRAUD_WH"
PRODUCER_ROLE = "FRAUD_FS_PRODUCER"
CONSUMER_ROLE = "FRAUD_FS_CONSUMER"
COMPUTE_POOL = "FRAUD_POOL"

STREAM_SOURCE = "TXN_STREAM"
FEATURE_GROUP = "FRAUD_FG"
FG_VERSION = "V1"
MODEL_NAME = "FRAUD_XGB"

# Decision thresholds on model score
REVIEW_THRESHOLD = 0.30
DECLINE_THRESHOLD = 0.70


# Account identifier (<orgname>-<account_name>) for PAT-based connections
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "<orgname>-<account_name>")


def get_session(use_pat: bool = os.getenv("FRAUD_USE_PAT") == "1") -> Session:
    """Create a Snowpark session.

    use_pat=True authenticates as the FRAUD_SVC service user with its PAT (no browser),
    which is what the simulator and the app use. Otherwise a named connection is used.
    """
    try:
        # Inside Snowflake Notebooks / Container Runtime an active session already exists
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
    except Exception:
        if use_pat:
            from pathlib import Path
            session = Session.builder.configs({
                "account": SNOWFLAKE_ACCOUNT,
                "user": "FRAUD_SVC",
                "authenticator": "PROGRAMMATIC_ACCESS_TOKEN",
                "token": (Path.home() / ".snowflake" / "fraud_svc.pat").read_text().strip(),
            }).create()
        else:
            conn = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
            session = Session.builder.config("connection_name", conn).create()
    session.use_role(PRODUCER_ROLE)
    session.use_warehouse(WAREHOUSE)
    session.use_database(DATABASE)
    session.sql("ALTER SESSION SET TIMEZONE = 'UTC'").collect()
    return session


def get_feature_store(session: Session):
    from snowflake.ml.feature_store import CreationMode, FeatureStore
    from snowflake.ml.feature_store.online_service import OnlineServiceAccess

    return FeatureStore(
        session=session,
        database=DATABASE,
        name=FS_SCHEMA,
        default_warehouse=WAREHOUSE,
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
        online_service_access=OnlineServiceAccess.PUBLIC,
    )
