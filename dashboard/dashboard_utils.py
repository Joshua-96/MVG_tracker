import pathlib as pl
import pandas as pd
import streamlit as st
import sys

sys.path.append(str(pl.Path(__file__).parent.parent))
from mvg_tracker.utils import get_connector, get_json_from_path, get_station_attributes
import datetime as dt
import json

REVERSE_REPLACE_MAP = {
                        "München Donnersbergerbrücke": "Donnersberger",
                        "Ost": "Ostbahnhof",
                        "-": "_",
                        " ": "_",
                        "ä": "ae",
                        "ö": "oe",
                        "ü": "ue",
                        "ß": "ss",
                        "Bahnhof": "Bf"
}


@st.cache
def load_df_from_db(dep_table_name: str):
    currPath = pl.Path(__file__).parent.parent
    jsonRelPath = "mvg_tracker/config/default_config.json"
    jsonPath = currPath.joinpath(jsonRelPath)
    config = get_json_from_path(jsonPath)
    conn = get_connector(**config["dbParams"])
    df = pd.read_sql_query(
                f'SELECT timestamp,station,line,destination,delay, curr_time_epoch, timestamp_epoch FROM public."{dep_table_name}"',
                conn,
                dtype={"timestamp": "datetime64",
                       "station": "str",
                       "line": "category",
                       "destination": "str",
                       "delay": "int16",
                       "curr_time_epoch": "uint64",
                       "timestamp_epoch": "uint64"
                        },
                )
    df["record_delta"] = (df["timestamp_epoch"] - df["curr_time_epoch"]).astype("int16")
    df.timestamp = pd.to_datetime(df.timestamp)
    df = df[df["record_delta"] < 180]
    df.sort_values("timestamp", ascending=False, inplace=True)
    # df.drop(columns=["curr_time_epoch", "timestamp_epoch", "record_delta","timestamp"],
    #         axis=1,
    #         inplace=True)
    # df["weekday"] = df["timestamp"].dt.weekday.astype("uint8")
    df["is_weekend"] = df["timestamp"].dt.weekday.between(5, 6)
    df["minute"] = df["timestamp"].dt.minute.astype("uint8")
    df = df.loc[:, ["timestamp", "station", "line",
                    "destination", "delay", "is_weekend"]]

    station_attr = get_station_attributes(config=config)

    # df["station"] = df["station"].replace("ö", "oe", regex=True)
    station_attr.replace({"station": REVERSE_REPLACE_MAP},
                         regex=True,
                         inplace=True)
    df.replace({"station": REVERSE_REPLACE_MAP},
               regex=True,
               inplace=True)
    for elmt in config["strip_list"]:
        station_attr["station"] = station_attr["station"].str.replace(
            elmt, "",
            regex=False)
        df["station"] = df["station"].str.replace(
            elmt, "",
            regex=False)
    df = df.merge(station_attr.loc[:, ["station", "lon", "lat", "Direction"]],
                  "left", on="station")
    
    df["station"] = df["station"].astype("category")
    df = df.reset_index(drop=True)
    return df


def filter_df(df: pd.DataFrame, axis: dict) -> pd.DataFrame:
    
    qry_str = ""

    for key, value in axis.items():
        if isinstance(value, list) or isinstance(value, tuple):
            if isinstance(value[0], str):
                qry_str = f"{qry_str} {key} in {value} &"
            # elif isinstance(value[0], dt.date) or isinstance(dt.time):
            elif isinstance(value[0], dt.date):
                qry_str = f"{qry_str} timestamp.dt.date >= @axis['{key}'][0] & timestamp.dt.date <= @axis['{key}'][1] &"
            elif isinstance(value[0], dt.time):
                qry_str = f"{qry_str} timestamp.dt.time >= @axis['{key}'][0] & timestamp.dt.time <= @axis['{key}'][1] &"
            
            else:

                if len(value) == 1:
                    qry_str = f"{qry_str} {key} == @axis['{key}'][0] &"
                else:
                    qry_str = f"{qry_str} {key} >= @axis['{key}'][0] & {key} <= @axis['{key}'][1] &"
        else:
            qry_str = f"{qry_str} {key} == {value} &"
    qry_str = qry_str[:-2]

    return df.query(qry_str)