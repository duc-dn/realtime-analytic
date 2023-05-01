import logging
import os

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")
LEVEL = {
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}

log_format = "[%(asctime)s|%(threadName)10s|%(levelname)7s|%(filename)s:%(lineno)03d] %(message)s"
formatter = logging.Formatter(log_format)

logging.basicConfig(level="INFO", format=log_format)

logger = logging.getLogger("hudi spark streaming")
