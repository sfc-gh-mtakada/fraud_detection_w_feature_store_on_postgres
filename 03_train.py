"""
03_train.py : Submit train/ (train_job.py + rt_features.py) as an ML Job on Container Runtime 3.0.

Run locally:  .venv/bin/python 03_train.py
"""
import sys

from snowflake.ml.jobs import submit_directory

import common as c


def submit(session, runtime):
    kwargs = {"runtime_environment": runtime} if runtime else {}
    return submit_directory(
        "train",
        c.COMPUTE_POOL,
        entrypoint="train_job.py",
        stage_name=f"{c.DATABASE}.{c.ML_SCHEMA}.JOB_STAGE",
        # Older runtimes ship an older snowflake-ml-python; Feature Groups need >= 2.5
        pip_requirements=None if runtime == "3.0" else ["snowflake-ml-python>=2.5", "httpx[http2]"],
        external_access_integrations=None if runtime == "3.0" else ["PYPI_ACCESS_INTEGRATION"],
        session=session,
        **kwargs,
    )


def main():
    session = c.get_session()
    session.sql(f"CREATE STAGE IF NOT EXISTS {c.DATABASE}.{c.ML_SCHEMA}.JOB_STAGE").collect()
    try:
        job = submit(session, "3.0")  # Container Runtime 3.0 (staged rollout from 2026-10-27)
    except Exception as ex:
        print(f"Runtime 3.0 not available yet in this account ({ex.__class__.__name__}); "
              "falling back to the default runtime")
        job = submit(session, None)
    print(f"Submitted ML Job {job.id}; waiting...")
    status = job.wait()
    print(job.get_logs())
    print(f"status={status}")
    sys.exit(0 if status == "DONE" else 1)


if __name__ == "__main__":
    main()
