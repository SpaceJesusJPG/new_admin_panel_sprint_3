import time
from typing import List, Union, Dict

from elasticsearch import Elasticsearch

from transform import transform_data

from etl.extract import Extractor
from etl.load import Loader
from utilities.configs import *
from utilities.connection_managers import pg_context
from utilities.storage import JsonFileStorage, State


class Runner:
    """Реализация основной логики работы приложения"""
    def __init__(self, extractors: List[Extractor]) -> None:
        self.extractors = extractors
        self.modified = None

    def check_modified(self): #-> Dict[str, Union[List[str], List]]:
        modified = {}
        for extractor in self.extractors:
            modified[extractor.table] = extractor.produce()



    #def some_name(self):
    #    if self.

# def load_filmwork():
#     fw_id_ls = extractor.produce()
#     records = fw_extractor.merge(fw_id_ls)
#     filmwork2elastic(transform_data(records))
#
#
# def load_person():
#     person_id_ls = s_extractor.produce()
#     person_fw_id_ls = p_extractor.enrich(person_id_ls)
#     records = p_extractor.merge(person_fw_id_ls)
#     filmwork2elastic(transform_data(records))
#
#
# def load_genre():
#     genre_id_ls = g_extractor.produce()
#     genre_fw_id_ls = g_extractor.enrich(genre_id_ls)
#     records = g_extractor.merge(genre_fw_id_ls)
#     filmwork2elastic(transform_data(records))


if __name__ == "__main__":
    with pg_context(POSTGRESQL_CONFIG) as pg_con:
        storage = JsonFileStorage(DUMP_PATH)
        state = State(storage)
        extractors = [Extractor(pg_con, table, state) for table in TABLES]
        es_con = Elasticsearch(ELASTIC_HOST)
        loader = Loader(es_con)

