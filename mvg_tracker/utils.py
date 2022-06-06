import pathlib as pl
import logging
import sqlalchemy as sa

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s : %(message)s'
DATEFORMAT = '%Y/%m/%d %I:%M:%S %p'


def init_file_logger(logger, log_dir, logLevel=logging.DEBUG):
    if isinstance(log_dir, str):
        log_dir = pl.Path(log_dir)
    log_file_handler = logging.FileHandler(log_dir.joinpath("log_file.txt"))
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATEFORMAT)
    log_file_handler.setFormatter(formatter)
    log_file_handler.setLevel(logLevel)
    logger.addHandler(log_file_handler)
    return logger


def init_console_logger(logger, logLevel=logging.DEBUG):
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s : %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p')
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.DEBUG)
    log_console_handler.setFormatter(formatter)
    logger.addHandler(log_console_handler)
    return logger


def get_connector(**config):
    srv = "postgresql"
    user = config["user"]
    pw = config["password"]
    host = config["host"]
    port = config["port"]
    db = config["database"]
    try:
        alchemyEngine = sa.create_engine(
            f'{srv}://{user}:{pw}@{host}:{port}/{db}',
            pool_recycle=3600,)
        alchemyEngine.connect()
    except sa.exc.OperationalError:
        self.logger.warning("fallback to localhost")
        alchemyEngine = sa.create_engine(
            f'{srv}://{user}:{pw}@localhost:{port}/{db}',
            pool_recycle=3600,)
        alchemyEngine.connect()
    return alchemyEngine.connect()