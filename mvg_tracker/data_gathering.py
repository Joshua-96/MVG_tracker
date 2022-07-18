import itertools
import signal
import time
from datetime import datetime
from numpy import int64
import pandas as pd
import pathlib as pl
import asyncio
import aiohttp
import sys
import logging
import os
from utils import get_station_attributes
from networking import cache_dep
from dataclasses import dataclass, fields
from utils import init_console_logger, init_file_logger, get_connector

# from apscheduler.schedulers.background import BackgroundScheduler


COL_ORDER = ["timestamp",
             "station",
             "line",
             "destination",
             "delay",
             "Id",
             "curr_time_epoch",
             "timestamp_epoch"]


@dataclass
class departure:
    timestamp: datetime
    station: str
    line: str
    destination: str
    delay: int
    Id: str
    curr_time_epoch: int64
    timestamp_epoch: int


class DataManager:

    def __init__(self,
                 config,
                 depTableName,
                 transTableName,
                 loggingDir=None,
                 backUpFolder=None):
        self.config = config
        self.depTableName = depTableName
        self.transTableName = transTableName
        self.backUpFolder = pl.Path(backUpFolder) \
            if backUpFolder is not None else None
        self.refreshInterval = 30
        self.saveInterval = 960
        self.backUpInterval = 39600
        self.cwd = str(pl.Path(__file__).parent)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger = init_console_logger(self.logger, logging.DEBUG)
        if loggingDir is not None:
            self.logger = init_file_logger(self.logger,
                                           loggingDir,
                                           logging.INFO)

        self.timeOffset = 2*60*3600
        self.load_trans_config()
        self.last_saved = datetime.today().date()
        self.db_connector = get_connector(**config["dbParams"])

    def init_stationID(self):
        self.staion_attr = get_station_attributes(self.config).loc[:, ["station", "ID"]]
        stations = self.staion_attr.station.to_list()
        keys = self.staion_attr.ID.to_list()
        self.stationID = dict(zip(stations, keys))

    def get_Df(self, cachedDep: dict, stationID: dict):
        depDf = pd.DataFrame(
            columns=[field.name for field in fields(departure)])
        for i, station in enumerate(stationID):
            querey = cachedDep[i]
            for dep in querey["departures"]:
                try:
                    if dep["product"] == "SBAHN"\
                            and (int(dep["departureTime"]/1000) >
                                 int(time.time()) - 3600)\
                            and "delay" in dep.keys():
                        timestamp_epoch = int(
                            dep["departureTime"]/1000) + int(dep["delay"]*60)
                        cur_dep = departure(
                            timestamp=datetime.fromtimestamp(
                                timestamp=timestamp_epoch).strftime(
                                    "%Y/%m/%d, %H:%M:%S"),
                            station=station,
                            line=dep["label"],
                            destination=dep["destination"],
                            delay=dep["delay"],
                            Id=dep["departureId"],
                            curr_time_epoch=int(
                                datetime.timestamp(datetime.now())),
                            timestamp_epoch=timestamp_epoch
                        )
                        cur_depDf = pd.DataFrame(data=[cur_dep])
                        depDf = pd.concat(
                            [depDf, cur_depDf], ignore_index=True)

                except KeyError:
                    self.logger.warning(
                        f'KeyError: {station} not found in with' +
                        f'line {dep["label"]}')
                    # self.logger.error(
                    #     ErrorType="KeyError",
                    #     station=station, line=dep["label"])
                    continue
        return depDf

    def calculate_transfer(self, transDf):

        def merge_reduce_Df(inboundDf, outboundDf, suffixes, hour_axis):
            merged = inboundDf.merge(outboundDf,
                                     how="inner",
                                     left_on=["date", "station", hour_axis],
                                     right_on=["date", "station", "hour"],
                                     suffixes=suffixes)
            del outboundDf
            merged = merged[merged[f"line{suffixes[0]}"]
                            != merged[f"line{suffixes[1]}"]]
            merged = merged[merged[f"timestamp{suffixes[0]}"]
                            < merged[f"timestamp{suffixes[1]}"]]
            merged["timestamp_epoch"] = merged[f"timestamp_epoch{suffixes[0]}"]
            merged.drop(f"timestamp_epoch{suffixes[0]}",
                        axis="columns",
                        inplace=True)
            if f"timestamp_epoch{suffixes[1]}" in list(merged.columns):
                merged.drop(
                    f"timestamp_epoch{suffixes[1]}",
                    axis="columns",
                    inplace=True)

            return merged

        transDf = transDf.replace({"destination": self.config["replaceMap"]}, regex=True)
        relevantStations = self.FileDict["transFile"].Bahnhof.drop_duplicates(
        ).values.tolist()
        transDf = transDf[transDf["station"].isin(relevantStations)]
        transDf.drop(["Id", "curr_time_epoch"], axis="columns", inplace=True)

        transDf = transDf.merge(self.staion_attr,
                                how="inner",
                                on="station"
                                )

        transDf = transDf.merge(self.FileDict["destFile"],
                                how="inner",
                                on="destination")

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

    def load_trans_config(self):
        self.FileDict = {i: None for i in self.config["assistFiles"]}
        for name in self.config["assistFiles"]:
            loadPath = os.path.join(self.cwd, self.config["assistFiles"][name])
            self.FileDict[name] = pd.read_csv(
                loadPath,
                sep=",",
                index_col=None,
                encoding="cp1252")
        self.init_stationID()
        # just station name and orientation is needed
        # self.FileDict["stationFile"].drop(["Breitengrad", "Laengengrad", "ID"],
        #                                   axis="columns",
        #                                   inplace=True)

    def loadDf(self):
        if self.db_connector is None:
            self.get_connector()
        return pd.read_sql_table(self.depTableName, self.db_connector)

    def save_df_datewise(self):
        for table, folder in [[self.transTableName, "trans"],
                              [self.depTableName, "dep"]]:
            df = pd.read_sql(
                f'SELECT * FROM public."{table}"',
                self.db_connector)
            if "timestamp" in df.columns:
                timestampCol = "timestamp"
            elif "timestamp_from" in df.columns:
                timestampCol = "timestamp_from"
            else:
                raise KeyError(f"no timestamp col found in {df.columns}")
            df[timestampCol] = pd.to_datetime(df[timestampCol])
            df["date"] = df[timestampCol].apply(lambda x: x.date(), 0)
            for unique_date in df.date.unique():
                dateStr = unique_date.strftime('%Y/%m/%d')
                FilePath = (self.backUpFolder
                            .joinpath(folder, dateStr)
                            .with_suffix('.csv'))
                FilePath.parent.mkdir(parents=True, exist_ok=True)
                df_date = df.loc[df["date"] == unique_date]
                df_date.drop("date", axis=1, inplace=True)
                if FilePath.exists():
                    incoming_df = pd.read_csv(FilePath, sep=",")
                    df_date = pd.concat([df_date, incoming_df],
                                        ignore_index=True)
                    del incoming_df
                    df_date.drop_duplicates()
                df_date.to_csv(str(FilePath), sep=",", index=False, mode="w")

    def create_db_table(self, Df, tableName):
        if self.db_connector is None:
            self.get_connector()
        Df.to_sql(tableName,
                  self.db_connector,
                  if_exists="append",
                  index=False)

    def update_db_table(self):
        if self.db_connector is None:
            self.get_connector()
        time_thresh = time.time()-self.timeOffset
        baseDf = pd.read_sql(
            f'SELECT * FROM public."{self.depTableName}"' +
            f'WHERE timestamp_epoch >= {time_thresh}',
            self.db_connector)
        self.db_connector.execute(
            f'DELETE FROM public."{self.depTableName}" WHERE timestamp_epoch' +
            f'>= {time_thresh}')
        baseDf = pd.concat([baseDf, self.cumDf], ignore_index=True)
        baseDf.drop_duplicates("Id", inplace=True, keep="last")
        transTable = self.calculate_transfer(baseDf)
        transTable = transTable.loc[:, [
            'delay_from',
            'hour',
            'next_hour_from',
            'date',
            'timestamp_to',
            'line_to',
            'destination_to',
            'delay_to',
            'next_hour_to',
            'timestamp_epoch',
            'hour_from',
            'hour_to']]
        baseTransDf = pd.read_sql(
            f'SELECT * FROM public."{self.transTableName}" WHERE ' +
            f'timestamp_epoch >= {time_thresh}',
            self.db_connector)
        baseTransDf = pd.concat([baseTransDf, transTable], ignore_index=True)
        baseTransDf.drop_duplicates(
            ["timestamp_from", "station", "line_from", "line_to"],
            ignore_index=True,
            inplace=True)
        self.db_connector.execute(
            f'DELETE FROM public."{self.transTableName}" WHERE ' +
            f'timestamp_epoch >= {time_thresh}')
        baseTransDf.to_sql(self.transTableName,
                           self.db_connector,
                           if_exists="append",
                           index=False)
        baseDf.to_sql(self.depTableName,
                      self.db_connector,
                      if_exists="append",
                      index=False)

    def signal_handler(self, signal, frame):
        self.update_db_table()
        self.save_df_datewise()
        self.logger.info("Saving data to backup-dir and shutting down")
        sys.exit(0)

    def clean_up_proc(self):
        self.update_db_table()

    async def get_df_by_chunks(self):
        for i in range(0, len(self.stationID), self.chunk_size):
            chunk = dict(itertools.islice(self.stationID.items(), i, i + self.chunk_size))
            cachedDep = await cache_dep(chunk, self.client)
            currDf = self.get_Df(cachedDep, chunk)
            if i == 0:
                cumDf = currDf
            else:
                cumDf = pd.concat([cumDf, currDf])
        return cumDf

    async def main(self, config):
        if config is None:
            raise ValueError("No Config given")

        loop = loop = asyncio.get_event_loop()
        self.client = aiohttp.ClientSession(loop=loop)
        self.chunk_size = 30
        self.cumDf = await self.get_df_by_chunks()
        self.create_db_table(self.cumDf, self.depTableName)
        transDf = self.calculate_transfer(self.cumDf)
        self.create_db_table(transDf, self.transTableName)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        while True:
            try:
                epoch_now = int(datetime.timestamp(datetime.now()))
                
                currDf = await self.get_df_by_chunks()
                self.cumDf = pd.concat([self.cumDf, currDf])
                self.cumDf = self.cumDf.drop_duplicates("Id", keep="last")
                self.cumDf.replace({"destination": self.config["replaceMap"]},
                                   regex=True,
                                   inplace=True)
                if epoch_now % self.backUpInterval < self.refreshInterval + 2:
                    self.save_df_datewise()
                    self.logger.info("executing planned db backup")
                    self.last_saved = datetime.today().date()

                epochTime_Df = int(
                    datetime.timestamp(
                        datetime.strptime(
                            currDf.timestamp[0].min(), "%Y/%m/%d, %H:%M:%S")
                        )
                    )

                self.logger.info(
                    "active connection with upcomming departure in" +
                    f"{epochTime_Df - epoch_now + 30} seconds, next " +
                    f"refresh in {self.refreshInterval} seconds")
                # sys.stdout.write(f"working {epochTime_Df - epoch_now} \r")

                if epochTime_Df > (epoch_now + 60):
                    time2wait = (epochTime_Df - epoch_now) - 30
                    time.sleep(time2wait)
                    self.logger.info(
                        f"sleepmode, waiting for {time2wait} seconds")

                time.sleep(self.refreshInterval)

                if epoch_now % self.saveInterval < self.refreshInterval+2:
                    self.logger.info(
                        "saving snapshot of Departures to database, saving" +
                        f"again in {self.saveInterval // 60} minutes")
                    self.update_db_table()
                    self.cumDf = self.cumDf[0:0]
                    time.sleep(30)

            except aiohttp.ServerConnectionError:
                print("ResponseError")
                self.logger.error("ResponseError")
                self.update_db_table()
                self.cumDf = self.cumDf[0:0]
                time.sleep(1)
                continue

            except AssertionError:
                self.logger.error("AssertionError")
                self.update_db_table()
                self.cumDf = self.cumDf[0:0]
                time.sleep(120)
                continue
            except aiohttp.ClientConnectionError:
                self.logger.error("InternetConnectionError")
                self.update_db_table()
                self.cumDf = self.cumDf[0:0]
                time.sleep(120)
                continue
            except IndexError:
                self.logger.error("PayloadError")
                time.sleep(30)
                continue
