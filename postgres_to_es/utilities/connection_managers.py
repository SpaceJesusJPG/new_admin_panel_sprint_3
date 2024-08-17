from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictConnection


@contextmanager
def pg_context(
    dsl: dict,
) -> psycopg2.extensions.connection:
    """Менеджер контекста подключения к PostgreSQL"""
    conn = psycopg2.connect(**dsl, connection_factory=DictConnection)
    try:
        yield conn
    finally:
        conn.close()
