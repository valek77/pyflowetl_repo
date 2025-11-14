import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import psutil
import os



_logger = None
_log_path = None
_logger_initialized_with = None

def set_log_file(path):
    global _log_path
    _log_path = path

def get_logger():
    global _logger, _log_path, _logger_initialized_with

    log_path = _log_path or os.path.join(os.path.dirname(__file__), 'pyflowetl.log')

    # Se già inizializzato con un path diverso, resetta
    if _logger and _logger_initialized_with != log_path:
        for handler in _logger.handlers[:]:
            _logger.removeHandler(handler)
        _logger = None

    if _logger is not None:
        return _logger

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger("pyflowetl")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # FILE HANDLER
    file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # CONSOLE HANDLER
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    _logger = logger
    _logger_initialized_with = log_path
    return logger


def log_memory_usage(label=""):
    logger = get_logger()
    process = psutil.Process(os.getpid())
    rss_mb = process.memory_info().rss / (1024 * 1024)
    logger.info(f"[Memoria] {label} - RAM usata: {rss_mb:.2f} MB")