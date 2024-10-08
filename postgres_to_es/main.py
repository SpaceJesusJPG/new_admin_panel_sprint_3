import logging
from logging import INFO

from etl.extract import Extractor
from etl.load import Loader
from runner import Runner
from utilities.configs import (
    DUMP_PATH,
)
from utilities.configs import TABLES, POSTGRESQL_CONFIG, ELASTIC_HOST
from utilities.pg_client import PGClient
from utilities.storage import JsonFileStorage, State

if __name__ == "__main__":
    logging.basicConfig(
        format="{asctime}:{levelname}:{name}: {message}",
        style="{",
        level=INFO,
    )
    logging.getLogger("elastic_transport").setLevel(logging.CRITICAL)

    storage = JsonFileStorage(DUMP_PATH)
    state = State(storage)
    pg_client = PGClient(POSTGRESQL_CONFIG)
    pg_client.validate_config()
    extractors = {table: Extractor(pg_client, table, state) for table in TABLES}
    loader = Loader(ELASTIC_HOST)
    runner = Runner(state, loader, extractors)
    runner.run_loop()
