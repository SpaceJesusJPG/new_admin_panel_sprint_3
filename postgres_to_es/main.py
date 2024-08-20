import logging
from logging import INFO

from extract import Extractor
from load import Loader
from runner import Runner
from utilities.configs import (
    DUMP_PATH,
)
from utilities.storage import JsonFileStorage, State
from utilities.configs import TABLES, POSTGRESQL_CONFIG
from utilities.connection_managers import PGClient

if __name__ == "__main__":
    logging.basicConfig(
        format="{asctime}:{levelname}:{name}:{message}",
        style="{",
        level=INFO,
    )
    logging.getLogger("elastic_transport").setLevel(logging.CRITICAL)

    storage = JsonFileStorage(DUMP_PATH)
    state = State(storage)
    pg_client = PGClient(POSTGRESQL_CONFIG)
    extractors = {
        table: Extractor(pg_client, table, state) for table in TABLES
    }
    loader = Loader(es_con)
    runner = Runner(state)
    runner.run_loop()
