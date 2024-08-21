import logging
import time
from typing import List, Any

from etl.transform import transform_data
from utilities.configs import GENRE_BATCH_SIZE


class Runner:
    """Реализация основной логики приложения."""

    def __init__(self, state, loader, extractors):
        self.state = state
        self.loader = loader
        self.extractors = extractors

    def extract_new_modified(self):
        """Получение всех обновленных данных из трех таблиц."""
        modified_id_dict = {}
        for extractor in self.extractors.values():
            modified_id_dict[extractor.table] = extractor.produce()
        return modified_id_dict

    def run_etl_filmwork(self, new_modified_ls: List[List[Any]]):
        """Трансформация и загрузка данных таблицы film_work."""

        extractor = self.extractors["film_work"]
        records = extractor.merge(new_modified_ls)
        self.loader.filmwork2elastic(transform_data(records))
        extractor.update_state(new_modified_ls)

    def run_etl_person(self, new_modified_ls: List[List[Any]]):
        """Трансформация и загрузка данных таблицы person."""
        extractor = self.extractors["person"]
        person_fw_id_ls = extractor.enrich(new_modified_ls)
        records = extractor.merge(person_fw_id_ls)
        self.loader.filmwork2elastic(transform_data(records))
        extractor.update_state(new_modified_ls)

    def run_etl_genre(self, new_modified_ls: List[List[Any]]):
        """Трансформация и загрузка данных таблицы genre пачками."""
        extractor = self.extractors["genre"]
        offset = 0
        while True:
            genre_fw_id_ls = extractor.enrich(new_modified_ls, GENRE_BATCH_SIZE, offset)
            if not genre_fw_id_ls:
                break
            records = extractor.merge(genre_fw_id_ls)
            self.loader.filmwork2elastic(transform_data(records))
            offset += GENRE_BATCH_SIZE
        extractor.update_state(new_modified_ls)

    def run_loop(self):
        """Основной цикл получения обновлений и их загрузки в Elasticsearch"""
        while True:
            new_modified = self.extract_new_modified()
            if new_modified["film_work"]:
                logging.info("Получено обновление таблицы film_work")
                self.run_etl_filmwork(new_modified["film_work"])
                logging.info(
                    "Загрузка обновлений таблицы film_work в Elasticsearch завершена"
                )
            if new_modified["person"]:
                logging.info("Получено обновление таблицы person")
                self.run_etl_person(new_modified["person"])
                logging.info(
                    "Загрузка обновлений таблицы person в Elasticsearch завершена"
                )
            if new_modified["genre"]:
                logging.info("Получено обновление таблицы genre")
                self.run_etl_genre(new_modified["genre"])
                logging.info(
                    "Загрузка обновлений таблицы genre в Elasticsearch завершена"
                )

            time.sleep(5)
