"""
BattETL logger module
"""

import os
import logging
import datetime
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

load_dotenv()

LOG_MAX_SIZE = 10 * 1024 * 1024  # 10 MB
STREAM_LOG_MAX_LENGTH = 1000
MODULE_DIR = os.path.abspath(os.path.dirname(__file__))
LOG_DIR = os.path.join(MODULE_DIR, "logs")
if not os.path.isdir(LOG_DIR):
    os.mkdir(LOG_DIR)
LOG_FILE_NAME = 'battetl.log'
LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

# Log format
# ----------
# YYYY-MM-DD HH:MM:SS LEVEL [MODULE:LINENO] MESSAGE
# asctime: Human-readable time when the LogRecord was created.
# levelname: Text logging level for the message ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
# module: The module (name portion of filename).
# lineno: Source line number where the logging call was issued (if available).
# message: The logged message.
logger = logging.getLogger('battetl')

log_format_stream = f'%(asctime)s [%(levelname)s][%(filename)s:%(lineno)d][%(name)s] %(message)s'
formatter_stream = logging.Formatter(fmt=log_format_stream)
handler_stream = logging.StreamHandler()
handler_stream.setFormatter(formatter_stream)
logger.addHandler(handler_stream)

log_format_file = '{ "time": "%(asctime)s", "timestamp": "%(created)f", "level": "%(levelname)s", "file": "%(filename)s:%(lineno)d", "name": "%(name)s", "thread": "%(threadName)s", "process": "%(process)d", "message": "%(message)s" }'
formatter_file = logging.Formatter(fmt=log_format_file)
handler_file = RotatingFileHandler(
    LOG_FILE_PATH, maxBytes=LOG_MAX_SIZE, backupCount=10, delay=True)
handler_file.setFormatter(formatter_file)
logger.addHandler(handler_file)


# Set log level to DEBUG if ENV is dev
env = os.getenv('ENV')
if env == 'dev':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
