"""
Database connection management for Neon Postgres.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

from config import DATABASE_URL


@contextmanager
def get_connection(schema="corpus"):
    """
    Yield a psycopg2 connection with search_path set to the given schema.
    Auto-commits on success, rolls back on exception.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set in .env")

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO %s, corpus, public;", (schema,))
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_dict_cursor(conn):
    """Return a cursor that yields dicts instead of tuples."""
    return conn.cursor(cursor_factory=RealDictCursor)
