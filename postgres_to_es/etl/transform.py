import uuid
from collections import defaultdict
from typing import List, Union

from pydantic import BaseModel, ValidationError


class Person(BaseModel):
    id: uuid.UUID
    name: str


class MovieIndex(BaseModel):
    id: uuid.UUID
    imdb_rating: Union[float, None]
    title: str
    description: Union[str, None]
    genres: List[str]
    actors_names: List[str]
    writers_names: List[str]
    directors_names: List[str]
    actors: List[Person]
    writers: List[Person]
    directors: List[Person]


def validate_data(
    data: defaultdict[str, dict[str, list]]
) -> defaultdict[str, dict[str, list]]:
    """Валидирует составленные документы."""
    for index_id, doc in data.items():
        try:
            data[index_id] = MovieIndex(**doc).model_dump()
        except ValidationError:
            data.pop(index_id)

    return data


def transform_data(records: List[tuple]) -> defaultdict[str, dict[str, list]]:
    """Трансформирует собранные записи в нужный для Elasticsearch формат."""
    data = defaultdict(
        lambda: {
            "actors_names": [],
            "writers_names": [],
            "directors_names": [],
            "actors": [],
            "writers": [],
            "directors": [],
            "genres": [],
        }
    )

    for row in records:
        (
            fw_id,
            title,
            description,
            rating,
            role,
            person_id,
            person_name,
            genre_name,
        ) = row

        if fw_id not in data:
            data[fw_id].update(
                {
                    "id": fw_id,
                    "title": title,
                    "description": description,
                    "imdb_rating": rating,
                }
            )

        dict_person = {
            "id": person_id,
            "name": person_name,
        }
        if (
            dict_person not in data[fw_id]["actors"]
            and dict_person not in data[fw_id]["writers"]
            and dict_person not in data[fw_id]["directors"]
        ) and person_id is not None:
            data[fw_id][role + "s"].append(dict_person)
            data[fw_id][role + "s_names"].append(person_name)

        if genre_name not in data[fw_id]["genres"]:
            data[fw_id]["genres"].append(genre_name)

    data = validate_data(data)

    return data
