from contextlib import contextmanager
from typing import Tuple

import psycopg2
from psycopg2.extras import DictConnection
from utilities.configs import POSTGRESQL_CONFIG, ELASTIC_HOST
from elasticsearch import Elasticsearch
from utilities.backoff import backoff


@contextmanager
def connection_context() -> Tuple[psycopg2.extensions.connection, Elasticsearch]:
    """Менеджер контекста подключения к базам данных"""
    pg_con = psycopg2.connect(**POSTGRESQL_CONFIG, connection_factory=DictConnection)
    es_con = Elasticsearch(ELASTIC_HOST)
    try:
        yield pg_con, es_con
    finally:
        pg_con.close()
        es_con.close()


class PGClient:
    def __init__(self, pg_config):
        self.pg_config = pg_config

    @backoff
    def execute_query(self, query):
        with psycopg2.connect(**self.pg_config, connection_factory=DictConnection) as conn:
            curr = conn.cursor()
            result = curr.execute(query)
            return result
