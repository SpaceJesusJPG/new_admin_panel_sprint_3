from typing import List, Any

from utilities.pg_client import PGClient
from utilities.storage import State


class Extractor:
    """Класс для получения последних измененных данных
    из указанной таблицы PosgreSQL."""

    def __init__(
            self,
            db_client: PGClient,
            table: str,
            state: State,
    ) -> None:
        self.db_client = db_client
        self.table = table
        self.state = state

    @staticmethod
    def value_or_null(value: str) -> str:
        if value:
            return value
        return "NULL"

    @staticmethod
    def get_id_str(records):
        """Трансформация списка id в формат, подходящий для вставки в запрос SQL"""
        id_ls = [record["id"] for record in records]
        id_str = ", ".join(f"'{s}'" for s in id_ls)
        return id_str

    def update_state(self, records):
        """Сохранение даты и времени последней полученной записи."""
        last_modified_dt = records[-1]["modified"]
        self.state.set_state(self.table, last_modified_dt)

    def produce(self) -> list[tuple[Any, ...]]:
        """Поиск последних измененных данных в нужной таблице."""
        produce_query = f"""
        SELECT id, modified
        FROM content.{self.table}
        WHERE modified > '{self.state.get_state(self.table)}'
        ORDER BY modified;
        """
        records = self.db_client.execute_pg_query(produce_query)
        return records

    def enrich(
            self, modified_id_ls: list[tuple[Any, ...]], batch_size=None, offset=0
    ) -> list[tuple[Any, ...]]:
        """Получение всех many-to-many полей, связанных
        с измененными данными."""
        enrich_query = f"""
        SELECT fw.id
        FROM content.film_work fw
        LEFT JOIN content.{self.table}_film_work mfw
        ON mfw.film_work_id = fw.id
        WHERE mfw.{self.table}_id IN ({self.get_id_str(modified_id_ls)})
        ORDER BY fw.modified
        LIMIT {self.value_or_null(batch_size)} OFFSET {offset};
        """
        records = self.db_client.execute_pg_query(enrich_query)
        return records

    def merge(self, m2m_id_ls: list[tuple[Any, ...]]) -> List[tuple]:
        """Получение всей недостающей информации,
        нужной для загрузки данных в документ
        индекса Elasticsearch."""
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
        WHERE fw.id IN ({self.get_id_str(m2m_id_ls)});
        """
        records = self.db_client.execute_pg_query(merge_query)
        return records
