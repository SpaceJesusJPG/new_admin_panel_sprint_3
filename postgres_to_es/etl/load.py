from elasticsearch import Elasticsearch, helpers

from utilities.backoff import backoff


class Loader:
    """Обработка подключения к Elasticsearch и bulk-загрузки данных в индекс."""

    def __init__(self, host: str):
        self.host = host

    @staticmethod
    def _generate_actions(records):
        """Генератор документов для загрузки в Elasticsearch."""
        for doc_id, doc_body in records.items():
            yield {
                "_op_type": "index",
                "_index": "movies",
                "_id": doc_id,
                "_source": doc_body,
            }

    @backoff()
    def filmwork2elastic(self, records):
        """Метод загрузки документов."""
        with Elasticsearch(self.host) as conn:
            helpers.bulk(conn, self._generate_actions(records))
