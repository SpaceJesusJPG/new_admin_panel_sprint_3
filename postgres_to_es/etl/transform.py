def transform_filmwork_data(records):
    filmworks_dict = {}

    for record in records:
        fm_id, rating, title, description = record

        if fm_id not in filmworks_dict:
            filmworks_dict[fm_id] = {
                'fm_id': fm_id,
                'title': title,
                'books': []
            }

    return list(filmworks_dict.values())
