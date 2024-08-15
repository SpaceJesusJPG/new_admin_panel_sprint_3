from elasticsearch import Elasticsearch


class Loader:
    def __init__(self, connection):
        self.connection = connection

    def fw2elastic(self, records):

        for row in records:
            self.connection.index(
                index='movies',
                id=dict(row)['id'],
                body=dict(row),
            )
