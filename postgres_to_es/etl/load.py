import logging

from elasticsearch import Elasticsearch, helpers

from utilities.backoff import backoff


class Loader:
    def __init__(self, host: str):
        self.host = host

    @staticmethod
    def _generate_actions(records):
        for doc_id, doc_body in records.items():
            yield {
                "_op_type": "index",
                "_index": "movies",
                "_id": doc_id,
                "_source": doc_body
            }

    @backoff()
    def filmwork2elastic(self, records):
        with Elasticsearch(self.host) as conn:
            helpers.bulk(conn, self._generate_actions(records))
