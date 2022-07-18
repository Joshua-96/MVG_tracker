import pathlib as pl
import logging
import sqlalchemy as sa
import json
import pandas as pd

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

def get_station_attributes(config):
    cur_path = pl.Path(__file__)
    base_path = cur_path.parent.parent.joinpath("mvg_tracker")
    station_list_path = base_path.joinpath(
        config["assistFiles"]["stationFile"])
    station_attr_path = base_path.joinpath(
        config["assistFiles"]["registered_stations"])
    east_west_mapping_path = base_path.joinpath(
        config["assistFiles"]["stationDirection"])
    station_subset = pd.read_csv(str(station_list_path),
                                 sep=",")["Abk"]
    east_west_mapping_df = pd.read_csv(
        str(east_west_mapping_path),
        sep=",",
        dtype={"Abk": str, "Direction": "category"})
    station_df = pd.read_csv(str(station_attr_path), sep=",")
    station_df["Laenge"] = station_df["Laenge"].astype("float32")
    station_df["Breite"] = station_df["Breite"].astype("float32")
    station_df = station_df.merge(east_west_mapping_df, "inner", "DS100")
    station_df.rename(
        columns={"Breite": "lat",
                 "Laenge": "lon",
                 "NAME": "station",
                 "IFOPT": "ID"},
        inplace=True)
    return station_df.loc[station_df["DS100"].isin(station_subset),
                          ["ID","station", "lon", "lat", "Direction"]]


def init_console_logger(logger, logLevel=logging.DEBUG):
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s : %(message)s',
        datefmt='%Y/%m/%d %I:%M:%S %p')
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
        alchemyEngine = sa.create_engine(
            f'{srv}://{user}:{pw}@localhost:{port}',
            pool_recycle=3600,)
        alchemyEngine.connect()
    return alchemyEngine.connect()


def get_json_from_path(Path: pl.Path):
    assert Path.suffix == ".json", \
        f"Incorrect Filetype given: {Path.suffix} expected json"

    if Path.exists():
        with open(Path, "r") as read_file:
            params = json.load(read_file)
    else:
        raise FileNotFoundError(f"at path {Path}")

    return params

