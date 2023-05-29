import pathlib as pl
import sqlalchemy as sa
import json
import pandas as pd
import datetime as dt


class datetime(dt.datetime):
    def __divmod__(self, delta):
        seconds = int((self - dt.datetime.min).total_seconds())
        remainder = dt.timedelta(
            seconds=seconds % delta.total_seconds(),
            microseconds=self.microsecond,
        )
        quotient = self - remainder
        return quotient, remainder

    def __floordiv__(self, delta):
        return divmod(self, delta)[0]

    def __mod__(self, delta):
        return divmod(self, delta)[1]


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
                          ["ID", "station", "lon", "lat", "Direction"]]


def get_connector(**config) -> sa.engine.Connection:
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
