from typing import List

import psycopg2

from utilities.storage import State


def sql_formatter(code):
    scode = ", ".join(f"'{s}'" for s in code)
    return scode


class Extractor:
    """Класс для получения последних измененных данных
    из указанной таблицы PosgreSQL."""

    def __init__(
        self,
        connection: psycopg2.extensions.connection,
        table: str,
        state: State,
    ) -> None:
        self.connection = connection
        self.table = table
        self.state = state


    #@staticmethod
    #def update_state(records):
    #    try:
    #        last_modified_

    def produce(self) -> List[str]:
        """Поиск последних измененных данных в нужной таблице."""
        cursor = self.connection.cursor()
        produce_query = f"""
        SELECT id, modified
        FROM content.{self.table}
        WHERE modified > '{self.state.get_state(self.table)}'
        ORDER BY modified
        LIMIT 1000;
        """
        cursor.execute(produce_query)
        records = cursor.fetchall()
        cursor.close()

        self.state.set_state(self.table, records[-1]["modified"])

        return [record["id"] for record in records]

    def enrich(self, modified_id_ls: List[str]) -> List[str]:
        """Получение всех many-to-many полей, связанных
        с измененными данными."""
        cursor = self.connection.cursor()
        enrich_query = f"""
        SELECT fw.id
        FROM content.film_work fw
        LEFT JOIN content.{self.table}_film_work mfw ON mfw.film_work_id = fw.id
        WHERE mfw.{self.table}_id IN ({sql_formatter(modified_id_ls)})
        ORDER BY fw.modified;
        """
        cursor.execute(enrich_query)
        records = cursor.fetchall()
        cursor.close()

        return [record["id"] for record in records]

    def merge(self, m2m_id_ls: List[str]) -> List[tuple]:
        """Получение всей недостающей информации,
        нужной для загрузки данных в документ
        индекса Elasticsearch."""
        cursor = self.connection.cursor()
        merge_query = f"""
        SELECT
         fw.id as fw_id, 
         fw.title, 
         fw.description, 
         fw.rating, 
         pfw.role, 
         p.id, 
         p.full_name,
         g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN ({sql_formatter(m2m_id_ls)}); 
        """
        cursor.execute(merge_query)
        records = cursor.fetchall()
        cursor.close()

        return records
