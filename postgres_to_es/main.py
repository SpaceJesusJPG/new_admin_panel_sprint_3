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


def extract_new_modified(loader, extractors):
    if not loader.ping_connection():
        raise ConnectionError("Подключение к Elasticsearch недоступно.")
    modified_id_dict = {}
    for extractor in extractors.values():
        modified_id_dict[extractor.table] = extractor.produce()
    return modified_id_dict


def run_etl_filmwork(id_ls, loader, extractors):
    records = extractors["film_work"].merge(id_ls)
    loader.filmwork2elastic(transform_data(records))


def run_etl_person(id_ls, loader, extractors):
    person_fw_id_ls = extractors["person"].enrich(id_ls)
    records = extractors["person"].merge(person_fw_id_ls)
    loader.filmwork2elastic(transform_data(records))


def run_etl_genre(id_ls, loader, extractors):
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


@backoff(logging)
def run_loop():
    # TODO разобраться с бекофом, настроить педантик
    with (
        pg_context(POSTGRESQL_CONFIG) as pg_con,
        Elasticsearch(ELASTIC_HOST) as es_con,
    ):
        extractors = {
            table: Extractor(pg_con, table, state) for table in TABLES
        }
        loader = Loader(es_con)

        while True:
            new_modified = extract_new_modified(loader, extractors)
            if new_modified["film_work"]:
                logging.info("Получено обновление таблицы film_work")
                run_etl_filmwork(new_modified["film_work"], loader, extractors)
            if new_modified["person"]:
                logging.info("Получено обновление таблицы person")
                run_etl_person(new_modified["person"], loader, extractors)
            if new_modified["genre"]:
                logging.info("Получено обновление таблицы genre")
                run_etl_genre(new_modified["genre"], loader, extractors)

            time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        format="{asctime}:{levelname}:{name}:{message}",
        style="{",
        level=INFO,
    )
    logging.getLogger("elastic_transport").setLevel(logging.CRITICAL)

    storage = JsonFileStorage(DUMP_PATH)
    state = State(storage)

    run_loop()
