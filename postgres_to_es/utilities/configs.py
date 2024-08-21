import os

from pydantic import BaseModel, Field, PositiveInt
from dotenv import load_dotenv

load_dotenv()


class PostgresConfig(BaseModel):
    host: str = Field("localhost", description="Имя хоста базы")
    port: PositiveInt = Field(5432, description="Порт хоста базы")
    dbname: str = Field(..., description="Имя базы")
    user: str = Field(..., description="Имя пользователя")
    password: str = Field(..., description="Пароль")


POSTGRESQL_CONFIG = {
    "dbname": os.environ.get("SQL_DB"),
    "user": os.environ.get("SQL_USER"),
    "password": os.environ.get("SQL_PASSWORD"),
    "host": os.environ.get("SQL_HOST"),
    "port": os.environ.get("SQL_PORT"),
}

ELASTIC_HOST = os.environ.get("ES_HOST", default="http://localhost:9200/")

DUMP_PATH = "storage_dump.json"
TABLES = ["film_work", "person", "genre"]
GENRE_BATCH_SIZE = 100
