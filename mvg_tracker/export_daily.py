import os
import pandas as pd
import pathlib as pl
from datetime import date, datetime

from psycopg2 import Timestamp
from data_gathering import get_json_from_path
from sqlalchemy import create_engine

from utils import get_connector

DEP_TABLE_NAME = "MVG1"
TRANS_TABLE_NAME = "MVG_Trans1"
BACKUP_FOLDER_CLIENT = pl.Path("/mnt/external/Data_IO/daily")
BACKUP_FOLDER_HOST = pl.Path(os.path.expanduser("~")).joinpath("AppData\Roaming\MVG_Tracker\daily")
DEP_FOLDER = "dep"
TRANS_FOLDER = "trans"

COL_ORDER = ["timestamp",
             "station",
             "line",
             "destination",
             "delay",
             "Id",
             "curr_time_epoch",
             "timestamp_epoch"]

def save_df_datewise(df, folder):
    if "timestamp" in df.columns:
        timestampCol = "timestamp"
    elif "timestamp_from" in df.columns:
        timestampCol = "timestamp_from"
    else:
        raise KeyError(f"no timestamp col found in {df.columns}")
    
    df[timestampCol] = pd.to_datetime(df[timestampCol])
    df["date"] = df[timestampCol].apply(lambda x: x.date(), 0)
    for unique_date in df.date.unique():
        dateStr = unique_date.strftime('%Y-%m-%d')
        FilePath = BACKUP_FOLDER_HOST.joinpath(folder, dateStr).with_suffix(f".csv")
        df_date = df.loc[df["date"] == unique_date]
        df_date.drop("date", axis=1, inplace=True)
        if FilePath.exists():
            incoming_df = pd.read_csv(FilePath, sep=",")
            df_date = pd.concat([df_date, incoming_df], ignore_index=True)
            del incoming_df
            df_date.drop_duplicates(subset=["Id"], inplace=True)
        df_date.to_csv(str(FilePath), sep=",", index=False)


def main():
    currPath = pl.Path(__file__).parent
    jsonRelPath = "config/default_config.json"
    jsonPath = currPath.joinpath(jsonRelPath)
    config = get_json_from_path(jsonPath)
    conn = get_connector(**config["dbParams"])
    # for loc, tbl_name in zip([DEP_FOLDER, TRANS_FOLDER],[DEP_TABLE_NAME, TRANS_TABLE_NAME]):
    #     df = pd.read_sql(
    #             f'SELECT * FROM public."{tbl_name}"',
    #             conn)
    #     # df.reindex(COL_ORDER)
    #     # df = pd.read_csv(BACKUP_FOLDER_HOST.parent.joinpath(DEP_TABLE_NAME).with_suffix(".csv"), sep=";")
    #     save_df_datewise(df, loc)
    df = pd.read_sql(
                f'SELECT * FROM public."{DEP_TABLE_NAME}"',
                conn)
    
    df.replace({"destination": config["replaceMap"]}, regex=True, inplace=True)
    df.to_sql("MVG1",
              conn,
              if_exists="replace",
              index=False)


if __name__ == "__main__":
    main()
