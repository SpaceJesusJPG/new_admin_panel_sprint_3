
from config.config import POSTGRESQL_CONFIG
from pprint import pp


def sql_formatter(code):
    scode = ", ".join(f"'{s}'" for s in code)
    return scode


class Extractor:
    def __init__(self, connection):
        self.connection = connection

    def produce(self, table, state, fields):
        cursor = self.connection.cursor()
        produce_query = f"""
        SELECT {', '.join(fields)}
        FROM content.{table}
        WHERE modified > '{state}'
        ORDER BY modified
        LIMIT 100; 
        """
        cursor.execute(produce_query)
        records = cursor.fetchall()
        cursor.close()

        return records

    def enrich(self, table, m2m_ids):
        cursor = self.connection.cursor()
        enrich_query = f"""
        SELECT fw.id, fw.modified
        FROM content.film_work fw
        LEFT JOIN content.{table}_film_work mfw ON mfw.film_work_id = fw.id
        WHERE mfw.{table}_id IN ({sql_formatter(m2m_ids)})
        ORDER BY fw.modified
        LIMIT 100; 
        """
        cursor.execute(enrich_query)
        records = cursor.fetchall()
        cursor.close()

        return records

    def merge(self, table, m2m_ids):
        cursor = self.connection.cursor()
        merge_query = f"""
        SELECT
         fw.id as fw_id, 
         fw.title, 
         fw.description, 
         fw.rating, 
         mfw.role, 
         p.id, 
         p.full_name
        FROM content.film_work fw
        LEFT JOIN content.{table}_film_work mfw ON mfw.film_work_id = fw.id
        LEFT JOIN content.{table} p ON p.id = mfw.{table}_id
        WHERE fw.id IN ({sql_formatter(m2m_ids)}); 
        """
        cursor.execute(merge_query)
        records = cursor.fetchall()
        cursor.close()

        return records


#def transform_data(data):
#    transformed_data = []
#
#    for row in data:
#
#        # Create a dictionary structure suitable for ES
#        transformed_data.append({
#            'id': row['fw_id'],
#            'imdb_rating': row['rating'],
#            'genres': {
#                row['name']
#            }
#        })
#
#    return transformed_data
#
#
#print(transform_data(data))
