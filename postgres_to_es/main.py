import logging
from logging import INFO

from runner import Runner
from utilities.configs import (
    DUMP_PATH,
)
from utilities.storage import JsonFileStorage, State

if __name__ == "__main__":
    logging.basicConfig(
        format="{asctime}:{levelname}:{name}:{message}",
        style="{",
        level=INFO,
    )
    logging.getLogger("elastic_transport").setLevel(logging.CRITICAL)

    storage = JsonFileStorage(DUMP_PATH)
    state = State(storage)
    runner = Runner(state)
    runner.run_loop()
