import logging
import time
from typing import List

from elasticsearch import Elasticsearch

from backoff import backoff
from configs import GENRE_BATCH_SIZE, POSTGRESQL_CONFIG, ELASTIC_HOST, TABLES
from connection_managers import connection_context
from extract import Extractor
from load import Loader
from transform import transform_data


class Runner:
    """Реализация основной логики приложения."""
    def __init__(self, state):
        self.state = state
        self.loader = None
        self. extractors = None

    def extract_new_modified(self):
        """Получение всех обновленных данных из трех таблиц."""
        if not self.loader.ping_connection():
            raise ConnectionError("Подключение к Elasticsearch недоступно")
        modified_id_dict = {}
        for extractor in self.extractors.values():
            modified_id_dict[extractor.table] = extractor.produce()
        return modified_id_dict

    def run_etl_filmwork(self, id_ls: List[str]):
        """Трансформация и загрузка данных таблицы film_work."""
        records = self.extractors["film_work"].merge(id_ls)
        self.loader.filmwork2elastic(transform_data(records))

    def run_etl_person(self, id_ls: List[str]):
        """Трансформация и загрузка данных таблицы person."""
        person_fw_id_ls = self.extractors["person"].enrich(id_ls)
        records = self.extractors["person"].merge(person_fw_id_ls)
        self.loader.filmwork2elastic(transform_data(records))

    def run_etl_genre(self, id_ls: List[str]):
        """Трансформация и загрузка данных таблицы genre пачками."""
        offset = 0
        while True:
            genre_fw_id_ls = self.extractors["genre"].enrich(
                id_ls, GENRE_BATCH_SIZE, offset
            )
            if not genre_fw_id_ls:
                break
            records = self.extractors["genre"].merge(genre_fw_id_ls)
            self.loader.filmwork2elastic(transform_data(records))
            offset += GENRE_BATCH_SIZE

    def run_etl(self):
        new_modified = self.extract_new_modified()
        if new_modified["film_work"]:
            logging.info("Получено обновление таблицы film_work")
            self.run_etl_filmwork(new_modified["film_work"])
        if new_modified["person"]:
            logging.info("Получено обновление таблицы person")
            self.run_etl_person(new_modified["person"])
        if new_modified["genre"]:
            logging.info("Получено обновление таблицы genre")
            self.run_etl_genre(new_modified["genre"])

    @backoff(logging)
    def run_loop(self):
        while True:
            new_modified = self.extract_new_modified()
            if new_modified["film_work"]:
                logging.info("Получено обновление таблицы film_work")
                self.run_etl_filmwork(new_modified["film_work"])
            if new_modified["person"]:
                logging.info("Получено обновление таблицы person")
                self.run_etl_person(new_modified["person"])
            if new_modified["genre"]:
                logging.info("Получено обновление таблицы genre")
                self.run_etl_genre(new_modified["genre"])
            time.sleep(1)
