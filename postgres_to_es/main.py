import time
import logging
from collections import deque
from logging import INFO

from elasticsearch import Elasticsearch

from etl.extract import Extractor
from etl.transform import transform_data
from etl.load import Loader
from utilities.configs import (
    POSTGRESQL_CONFIG,
    ELASTIC_HOST,
    DUMP_PATH,
    TABLES,
    GENRE_BATCH_SIZE,
)
from utilities.connection_managers import pg_context
from utilities.storage import JsonFileStorage, State
from utilities.backoff import backoff


@backoff()
def run(logger):
    def extract_new_modified():
        # TODO check_connection()
        modified_id_dict = {}
        for extractor in extractors.values():
            modified_id_dict[extractor.table] = extractor.produce()

        return modified_id_dict

    def run_etl_filmwork(id_ls):
        records = extractors["film_work"].merge(id_ls)
        loader.filmwork2elastic(transform_data(records))

    def run_etl_person(id_ls):
        person_fw_id_ls = extractors["person"].enrich(id_ls)
        records = extractors["person"].merge(person_fw_id_ls)
        loader.filmwork2elastic(transform_data(records))

    def run_etl_genre(id_ls):
        offset = 0
        while True:
            genre_fw_id_ls = extractors["genre"].enrich(
                id_ls, GENRE_BATCH_SIZE, offset
            )
            if not genre_fw_id_ls:
                break
            records = extractors["genre"].merge(genre_fw_id_ls)
            loader.filmwork2elastic(transform_data(records))
            offset += GENRE_BATCH_SIZE

    with pg_context(POSTGRESQL_CONFIG) as pg_con:
        storage = JsonFileStorage(DUMP_PATH)
        state = State(storage)
        extractors = {
            table: Extractor(pg_con, table, state) for table in TABLES
        }
        es_con = Elasticsearch(ELASTIC_HOST)
        loader = Loader(es_con)

        new_modified_queue = deque()

        while True:
            new_modified = extract_new_modified()
            new_modified_queue.append(new_modified)
            try:
                new_modified = new_modified_queue.pop()
                if new_modified["film_work"]:
                    run_etl_filmwork(new_modified["film_work"])
                    logger.info('Получено обновление таблицы film_work')

                if new_modified["person"]:
                    run_etl_person(new_modified["person"])
                    logger.info('Получено обновление таблицы person')

                if new_modified["genre"]:
                    run_etl_genre(new_modified["genre"])
                    logger.info('Получено обновление таблицы genre')
            except Exception as e:
                new_modified_queue.append(new_modified)
                logging.error(e)

            time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        format='{asctime}:{levelname}:{name}:{message}',
        style='{',
        level=INFO,
    )
    try:
        run(logging)
    except Exception as e:
        logging.error(f"При выполнении функции возникло исключение: {e}")
