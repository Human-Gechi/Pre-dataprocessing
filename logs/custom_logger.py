# logs/custom_logger.py

import os
import logging

# Get the path to the logs folder
log_dir = os.path.dirname(__file__)  # This resolves to the logs/ folder
log_path = os.path.join(log_dir, 'apps.log')  # apps.log will be in logs/

# Set up basic logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=log_path,
    filemode="a",
)

# Create logger instance
logger = logging.getLogger(__name__)
