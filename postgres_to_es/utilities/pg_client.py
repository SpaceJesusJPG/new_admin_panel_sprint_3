import logging
from contextlib import contextmanager
from typing import Tuple, List

import psycopg2
from psycopg2.extras import DictConnection

from utilities.backoff import backoff
from utilities.configs import POSTGRESQL_CONFIG


class PGClient:
    def __init__(self, pg_config):
        self.pg_config = pg_config

    @contextmanager
    def connect(self) -> psycopg2.extensions.cursor:
        """Менеджер контекста подключения к PostgreSQL"""
        pg_con = psycopg2.connect(**POSTGRESQL_CONFIG, connection_factory=DictConnection)
        cur = pg_con.cursor()
        try:
            yield cur
        finally:
            cur.close()
            pg_con.close()

    @backoff()
    def execute_pg_query(self, query) -> List[Tuple]:

        with self.connect() as cur:
            cur.execute(query)
            return cur.fetchall()
