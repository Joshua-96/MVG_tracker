from distutils.command.config import config
import os
import pandas as pd
import pathlib as pl
import sys
from datetime import date, datetime

from psycopg2 import Timestamp
from requests import get
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

def calculate_transfer(self, transDf):

    transDf = transDf.replace({"destination": self.replaceMap}, regex=True)
    relevantStations = self.FileDict["transFile"].Bahnhof.drop_duplicates(
    ).values.tolist()
    transDf = transDf[transDf["station"].isin(relevantStations)]
    transDf.drop(["Id", "curr_time_epoch"], axis="columns", inplace=True)

    transDf = transDf.merge(self.FileDict["stationFile"],
                            how="inner",
                            on="station"
                            )

    transDf = transDf.merge(self.FileDict["destFile"],
                            how="inner",
                            on=["destination", "Himmelsrichtung"])

    transDf.drop("Himmelsrichtung", axis="columns", inplace=True)
    transDf.timestamp = pd.to_datetime(transDf.timestamp)
    transDf["hour"] = transDf.timestamp.dt.hour
    transDf["next_hour"] = transDf.hour + 1
    transDf["date"] = transDf.timestamp.dt.date

    inboundDf = transDf[transDf.Richtung == "einwärts"]
    outboundDf = transDf[transDf.Richtung == "auswärts"]
    suffixes = ["_from", "_to"]
    del transDf
    inboundDf.drop("Richtung", axis="columns", inplace=True)
    outboundDf.drop("Richtung", axis="columns", inplace=True)

    inbound_currDf = merge_reduce_Df(
        inboundDf, outboundDf, suffixes, "hour")

    inbound_nextDf = merge_reduce_Df(
        inboundDf, outboundDf, suffixes, "next_hour")

    combDf = pd.concat([inbound_currDf, inbound_nextDf])
    del inbound_currDf, inbound_nextDf

    combDf = combDf[
        combDf[f"timestamp{suffixes[0]}"] < combDf[f"timestamp{suffixes[1]}"]]

    combDf.sort_values(by=[f"timestamp{suffixes[0]}",
                            f"timestamp{suffixes[1]}"],
                        inplace=True)
    combDf.drop_duplicates([f"timestamp{suffixes[0]}",
                            "station",
                            f"line{suffixes[0]}",
                            f"line{suffixes[1]}"],
                            keep="first",
                            inplace=True)

    return combDf

def load_df_datewise(conn, folder):

    for csvFile in BACKUP_FOLDER_HOST.joinpath(folder).glob("*.csv"):
        #dateStr = datetime.strptime(csvFile.stem, '%Y-%m-%d')
        dateStr = csvFile.stem.replace("-", "/")
        temp_csv = pd.read_csv(csvFile, sep=";")
        if "timestamp" in temp_csv.columns:
            timestampCol = "timestamp"
        elif "timestamp_from" in temp_csv.columns:
            timestampCol = "timestamp_from"
        else:
            raise KeyError(f"no timestamp col found in {df.columns}")
        sql_df = pd.read_sql(
            f'SELECT * FROM public."{TRANS_TABLE_NAME}"' +
                f" WHERE {timestampCol} LIKE '%%{dateStr}%%'",
            conn)
        
        temp_csv.timestamp = temp_csv.timestamp.str.replace("-", "/")
        temp_csv.station = temp_csv.station.str.replace(", ", "_")
        temp_csv.station = temp_csv.station.str.replace("/","")
        temp_csv.drop_duplicates(["Id"], inplace=True)
        if sql_df.count()[0] == 0:
            temp_csv.to_sql("MVG1",
                            conn,
                            if_exists="append",
                            index=False)
            continue
        elif sql_df.count()[0] == temp_csv.count()[0]:
            continue
        combDf = pd.concat([temp_csv, sql_df])
   
        combDf.drop_duplicates(subset=["Id"], inplace=True)
        conn.execute( f'DELETE FROM public."{TRANS_TABLE_NAME}"' +
                f" WHERE timestamp LIKE '%%{dateStr}%%'")
        
        combDf.to_sql("MVG1",
                    conn,
                    if_exists="append",
                    index=False)
        combDf.to_csv(csvFile, ",", index=False)

def main():
    currPath = pl.Path(__file__).parent
    jsonRelPath = "config/default_config.json"
    jsonPath = currPath.joinpath(jsonRelPath)
    config = get_json_from_path(jsonPath)
    conn = get_connector(**config["dbParams"])
    
    # df = pd.read_csv(BACKUP_FOLDER_HOST.parent.joinpath(DEP_TABLE_NAME).with_suffix(".csv"), sep=";")
    load_df_datewise(conn, TRANS_FOLDER)



if __name__ == "__main__":
    main()
