import psycopg2
from psycopg2.extras import DictConnection
from config.config import POSTGRESQL_CONFIG
from pprint import pp
import pandas as pd


def sql_formatter(code):
    scode = ", ".join(f"'{s}'" for s in code)
    return scode


class Extractor:
    def __init__(self, connection):
        self.connection = connection

    def produce(self, table):
        cursor = self.connection.cursor()
        query = f"""
        SELECT id, modified
        FROM content.{table}
        WHERE modified > '2024-08-15 02:15:00'
        ORDER BY modified
        LIMIT 100; 
        """
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()

        return records

    def enrich(self, table, person_ids):
        cursor = self.connection.cursor()
        query_enrich = f"""
        SELECT fw.id, fw.modified
        FROM content.film_work fw
        LEFT JOIN content.{table}_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.{table}_id IN ({sql_formatter(person_ids)})
        ORDER BY fw.modified
        LIMIT 100; 
        """
        cursor.execute(query_enrich)
        records = cursor.fetchall()
        cursor.close()

        return records

    def merge(self, filmwork_ids):
        cursor = self.connection.cursor()
        query_merge = f"""
        SELECT
         fw.id as fw_id, 
         fw.title, 
         fw.description, 
         fw.rating, 
         fw.type, 
         fw.created, 
         fw.modified, 
         pfw.role, 
         p.id, 
         p.full_name,
         g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN ({sql_formatter(filmwork_ids)}); 
        """
        cursor.execute(query_merge)
        records = cursor.fetchall()
        cursor.close()

        return records

    def extract(self, table):
        person_ids = [person['id'] for person in self.produce(table)]
        filmworks_ids = [filmwork['id'] for filmwork in self.enrich(table, person_ids)]
        return self.merge(filmworks_ids)


con = psycopg2.connect(**POSTGRESQL_CONFIG, connection_factory=DictConnection)
extractor = Extractor(con)
data = extractor.extract('person')
for i in data:
    pp(dict(i))


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
