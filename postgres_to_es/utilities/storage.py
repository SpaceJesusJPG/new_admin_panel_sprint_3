import json
from json.decoder import JSONDecodeError
from typing import Any, Dict
from datetime import datetime


class JsonFileStorage:
    """Реализация хранилища состояния, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, str]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w+') as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, str]:
        """Получить состояние из хранилища."""
        try:
            with open(self.file_path, 'r') as file:
                data = json.load(file)
                return data
        except FileNotFoundError:
            return {}


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: datetime) -> None:
        """Установить состояние для определённого ключа."""
        state = self.storage.retrieve_state()
        state[key] = value.strftime('%Y-%m-%d %H:%M:%S.%f')
        self.storage.save_state(state)

    def get_state(self, key: str) -> datetime:
        """Получить состояние по определённому ключу."""
        state = self.storage.retrieve_state()
        try:
            return state[key]
        except KeyError:
            return datetime.min
