"""Apply a SQL file to the FRAUD_OLTP Postgres instance via ~/.pg_service.conf + ~/.pgpass."""
import sys

import psycopg2

PG_SERVICE = "fraud_oltp"


def main(path: str):
    with psycopg2.connect(service=PG_SERVICE) as conn, conn.cursor() as cur:
        cur.execute(open(path).read())
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'fraud' ORDER BY 1")
        print("fraud schema tables:", [r[0] for r in cur.fetchall()])


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "00_postgres_oltp.sql")
