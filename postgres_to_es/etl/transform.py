from collections import defaultdict
from typing import List


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

    return data
