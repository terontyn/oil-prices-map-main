import logging
import sys
import os
from logging.handlers import RotatingFileHandler

def setup_logging(log_file="amn.log"):
    log_format = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(module)-10s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger("AMN")
    logger.setLevel(logging.DEBUG)
    
    if logger.hasHandlers():
        logger.handlers.clear()

    # В консоль (для Docker logs)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # В файл (с ротацией)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    return logger

logger = logging.getLogger("AMN")
