import os
from dotenv import load_dotenv

load_dotenv()

POSTGRESQL_CONFIG = {
    'dbname': os.environ.get('SQL_DB'),
    'user': os.environ.get('SQL_USER'),
    'password': os.environ.get('SQL_PASSWORD'),
    'host': os.environ.get('SQL_HOST', default='localhost'),
    'port': os.environ.get('SQL_PORT', default=5432),
}

ELASTICSEARCH_CONFIG = {
    'host': os.environ.get('ES_HOST', default='localhost'),
    'port': os.environ.get('ES_PORT', default=9200),
}
