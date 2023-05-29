import logging
import pathlib as pl
from typing import Union


LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s : %(message)s'
DATEFORMAT = '%Y/%m/%d %I:%M:%S %p'


def init_file_logger(logger: logging.Logger, log_dir: pl.Path | str, logLevel=logging.DEBUG
                     ) -> logging.Logger:
    if isinstance(log_dir, str):
        log_dir = pl.Path(log_dir)
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            return logger
    log_file_handler = logging.FileHandler(log_dir.joinpath("log_file.txt"))
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATEFORMAT)
    log_file_handler.setFormatter(formatter)
    log_file_handler.setLevel(logLevel)
    logger.addHandler(log_file_handler)
    return logger


def init_console_logger(logger: logging.Logger, logLevel=logging.DEBUG) -> logging.Logger:
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s : %(message)s',
        datefmt='%Y/%m/%d %I:%M:%S %p')
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            return logger
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.DEBUG)
    log_console_handler.setFormatter(formatter)
    logger.addHandler(log_console_handler)
    return logger
