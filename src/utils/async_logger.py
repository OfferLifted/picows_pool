import logging
from asyncio import Event
from logging import Formatter, StreamHandler
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
from typing import Literal


async def init_logger(
    log_level: Literal[
        "CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "NOTSET"
    ] = "INFO",
):
    logging.getLogger("picows").setLevel(
        logging.WARNING
    )  # gets rid of picows INFO logs
    log = logging.getLogger()
    q = Queue()

    queue_handler = QueueHandler(q)
    log.addHandler(queue_handler)
    log.setLevel(logging.getLevelNamesMapping()[log_level])

    stream_handler = StreamHandler()
    formatter = Formatter("%(asctime)s - %(levelname)s - %(message)s")
    stream_handler.setFormatter(formatter)

    listener = QueueListener(q, stream_handler)

    wait_event = Event()
    try:
        listener.start()
        while True:
            await wait_event.wait()
    finally:
        listener.stop()
