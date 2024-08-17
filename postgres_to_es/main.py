from elasticsearch import Elasticsearch

from utilities.configs import *
from utilities.connection_managers import pg_context
from utilities.storage import JsonFileStorage, State
from etl.extract import Extractor
from etl.load import Loader
from etl.transform import transform_data


storage = JsonFileStorage(DUMP_PATH)
state = State(storage)


with pg_context(POSTGRESQL_CONFIG) as pg_con:
    es_con = Elasticsearch(ELASTIC_HOST)
    fw_extractor = Extractor(pg_con, FILM_WORK_TABLE, state)
    person_extractor = Extractor(pg_con, PERSON_TABLE, state)
    genre_extractor = Extractor(pg_con, GENRE_TABLE, state)
    loader = Loader(es_con)

    #while True:
    try:
        fw_ids = fw_extractor.produce()
        records = fw_extractor.merge(fw_ids)
        loader.filmwork2elastic(transform_data(records))

        person_ids = person_extractor.produce()
        person_fw_ids = person_extractor.enrich(person_ids)
        records = person_extractor.merge(person_fw_ids)
        loader.filmwork2elastic(transform_data(records))

    except IndexError:
        print('No new entries.')
#
    #    time.sleep(5)

# person_ids = person_extractor.produce()
# person_fw_ids = person_extractor.enrich(person_ids)
# records = person_extractor.merge(person_fw_ids)
# loader.filmwork2elastic(transform_data(records))

# genre_ids = genre_extractor.produce()
# genre_fw_ids = genre_extractor.enrich(genre_ids)
# records = genre_extractor.merge(genre_fw_ids)
# loader.filmwork2elastic(transform_data(records))

# print(person_extractor.state)
# print(genre_extractor.state)
