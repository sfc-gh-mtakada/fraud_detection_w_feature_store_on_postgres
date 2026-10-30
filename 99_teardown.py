"""
99_teardown.py : Stop everything that bills while idle.

  --pause (default) : drop the online service, suspend Postgres and the compute pool
                      (feature views, data, model and Postgres data are kept)
  --all             : additionally drop the database, Postgres instance, service user and policies

Recreate the online service later with 02_feature_store.py (FVs re-sync automatically).
"""
import argparse

import common as c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="drop every demo object")
    args = ap.parse_args()

    session = c.get_session()
    fs = c.get_feature_store(session)
    if fs.get_online_service_status().status != "NOT_FOUND":
        print(fs.drop_online_service())
    session.use_role("ACCOUNTADMIN")
    for sql in ("ALTER POSTGRES INSTANCE FRAUD_OLTP SUSPEND",
                f"ALTER COMPUTE POOL {c.COMPUTE_POOL} SUSPEND"):
        try:
            session.sql(sql).collect()
            print(f"ok: {sql}")
        except Exception as ex:
            print(f"skip: {sql} ({str(ex)[:100]})")

    if args.all:
        for sql in ("DROP POSTGRES INSTANCE IF EXISTS FRAUD_OLTP",
                    f"DROP COMPUTE POOL IF EXISTS {c.COMPUTE_POOL}",
                    "DROP USER IF EXISTS FRAUD_SVC",
                    "DROP NETWORK POLICY IF EXISTS FRAUD_SVC_POLICY",
                    "DROP NETWORK POLICY IF EXISTS FRAUD_PG_POLICY",
                    f"DROP DATABASE IF EXISTS {c.DATABASE}",
                    f"DROP WAREHOUSE IF EXISTS {c.WAREHOUSE}",
                    f"DROP ROLE IF EXISTS {c.PRODUCER_ROLE}",
                    f"DROP ROLE IF EXISTS {c.CONSUMER_ROLE}"):
            session.sql(sql).collect()
            print(f"ok: {sql}")


if __name__ == "__main__":
    main()
