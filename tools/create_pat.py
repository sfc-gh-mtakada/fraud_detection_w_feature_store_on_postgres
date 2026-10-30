"""Create (or rotate) the FRAUD_SVC PAT and store it at ~/.snowflake/fraud_svc.pat (0600).

The token value is never printed.
"""
import os
from pathlib import Path

from snowflake.snowpark import Session

PAT_PATH = Path.home() / ".snowflake" / "fraud_svc.pat"


def main():
    session = Session.builder.config("connection_name", os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")).create()
    session.use_role("ACCOUNTADMIN")
    session.sql("ALTER USER FRAUD_SVC REMOVE PROGRAMMATIC ACCESS TOKEN FRAUD_DEMO_PAT").collect() \
        if session.sql("SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER FRAUD_SVC").filter("\"name\" = 'FRAUD_DEMO_PAT'").count() else None
    row = session.sql(
        "ALTER USER FRAUD_SVC ADD PROGRAMMATIC ACCESS TOKEN FRAUD_DEMO_PAT "
        "ROLE_RESTRICTION = 'FRAUD_FS_PRODUCER' DAYS_TO_EXPIRY = 30"
    ).collect()[0]
    secret = row["token_secret"]
    PAT_PATH.parent.mkdir(exist_ok=True)
    fd = os.open(PAT_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(secret)
    print(f"PAT saved to {PAT_PATH} (expires in 30 days)")


if __name__ == "__main__":
    main()
