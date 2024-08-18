import os

from dotenv import load_dotenv

load_dotenv()

POSTGRESQL_CONFIG = {
    "dbname": os.environ.get("SQL_DB"),
    "user": os.environ.get("SQL_USER"),
    "password": os.environ.get("SQL_PASSWORD"),
    "host": os.environ.get("SQL_HOST", default="localhost"),
    "port": os.environ.get("SQL_PORT", default=5432),
}

ELASTIC_HOST = os.environ.get("ES_HOST", default="http://localhost:9200/")

DUMP_PATH = "storage_dump.json"
TABLES = ["film_work", "person", "genre"]
GENRE_BATCH_SIZE = 100
