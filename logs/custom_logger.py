import logging
import os

LOG_PATH = os.path.join(os.path.dirname(__file__), "apps.log")

logging.basicConfig(
    level= logging.DEBUG,
    filename = LOG_PATH,
    filemode = "w",
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)