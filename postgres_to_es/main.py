import psycopg2
from psycopg2.extras import DictConnection
from config.config import POSTGRESQL_CONFIG, ELASTIC_HOST
from etl.extract import Extractor
from elasticsearch import Elasticsearch
from etl.load import Loader
from pprint import pp


pg_con = psycopg2.connect(**POSTGRESQL_CONFIG, connection_factory=DictConnection)
es_con = Elasticsearch(ELASTIC_HOST)

extractor = Extractor(pg_con)
loader = Loader(es_con, )

state = '2024-08-15 02:15:00'


def load_fw():
    table = 'film_work'
    fields = ['id', 'rating as imdb_rating', 'title', 'description']
    records = extractor.produce(table, state, fields)
    loader.fw2elastic(records)


def load_person():
    table = 'person'
    records = extractor.produce(table, state, ['id', 'modified'])
    person_ids = [record['id'] for record in records]
    records = extractor.enrich(table, person_ids)
    person_fw_ids = [record['id'] for record in records]
    records = extractor.merge(table, person_fw_ids)
    print(*records)

load_person()
