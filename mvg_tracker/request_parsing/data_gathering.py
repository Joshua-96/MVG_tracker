import itertools
import signal
import time as inbuild_time
from datetime import timedelta, time
from numpy import int64
import pandas as pd
import pathlib as pl
import asyncio
import aiohttp
import sys
import logging
import os
from dataclasses import dataclass, fields
import sqlalchemy as sa
import numpy as np
from typing import Any
from mvg_tracker.request_parsing.networking import cache_dep
from mvg_tracker.request_parsing.data_classes import StationResponse
from mvg_tracker.request_parsing.enum_classes import Product
from mvg_tracker.data_validation.utils import get_connector, datetime
from mvg_tracker.logging_util.init_loggers import init_console_logger, init_file_logger

# from apscheduler.schedulers.background import BackgroundScheduler


COL_ORDER = ["timestamp",
             "station",
             "line",
             "destination",
             "delay",
             "Id",
             "curr_time_epoch",
             "timestamp_epoch"]
TIME_DELTA_THRESH = timedelta(hours=1)


@dataclass
class Departure:
    time_of_dep: datetime
    line_id: int
    delay: int
    time_of_record: datetime
    station_id: str
    destination_id: str


class DataManager:
    config: dict
    db_connector: sa.engine.Connection
    db_station: pd.DataFrame
    db_line: pd.DataFrame
    db_station_order: pd.DataFrame
    db_transition: pd.DataFrame

    def __init__(self,
                 config,
                 loggingDir=None,
                 backUpFolder=None):
        self.config = config
        self.db_connector = get_connector(**config["dbParams"])
        self.depTableName = config["dbTables"]["departures"]
        self.db_station = pd.read_sql_table(
            config["dbTables"]["station"], self.db_connector)
        self.db_line = pd.read_sql_table(
            config["dbTables"]["line"], self.db_connector)
        self.db_staion_order = pd.read_sql_table(
            config["dbTables"]["station_order"], self.db_connector)
        self.all_stations_names: np.ndarray = self.db_station.name.to_numpy(
            dtype="str")
        self.all_stations_ids: np.ndarray[Any, np.dtype[np.int32]] = self.db_station.station_id.to_numpy(
            dtype=np.int32)
        self.db_transition = pd.read_sql_table(
            config["dbTables"]["transition"], self.db_connector)
        self.backUpFolder = pl.Path(backUpFolder) \
            if backUpFolder is not None else None
        self.refreshInterval = timedelta(seconds=30).seconds
        self.saveInterval = timedelta(minutes=15).seconds
        self.backUpInterval = timedelta(hours=3)
        self.cwd = str(pl.Path(__file__).parent)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger = init_console_logger(self.logger, logging.DEBUG)
        if loggingDir is not None:
            self.logger = init_file_logger(self.logger,
                                           loggingDir,
                                           logging.INFO)
        self.init_stationID()
        self.last_saved = datetime.today().date()

    def init_stationID(self):
        # self.staion_attr = get_station_attributes(self.config).loc[:, ["station", "ID"]]
        stations = self.db_station.name.to_list()
        keys: list[str] = [str(s)
                           for s in self.db_station.station_id.to_list()]
        keys = ["de:0" + s[:4] + ":" + s[4:] for s in keys]
        self.stationID = dict(zip(stations, keys))

    def get_Df(self, cachedDep: list[dict], stationID: dict[str, str]):
        depDf = pd.DataFrame(
            columns=[field.name for field in fields(Departure)])
        for i, station in enumerate(stationID):
            querey: dict = cachedDep[i]
            if querey is None:
                continue
            stationResponse = StationResponse(querey)
            for dep in stationResponse.departures:
                try:
                    if dep.transportType != Product.SBahn\
                            or dep.get_time_to_dep() > TIME_DELTA_THRESH\
                            or dep.delay is None:
                        continue
                    dep.set_station_id(stationID[station])
                    dep.set_departure_id()
                    dep.set_destinationId_by_name(
                        self.all_stations_names, self.all_stations_ids)
                    cur_depDf = dep.get_df_repr("time_of_dep",
                                                "time_of_record",
                                                "line_id",
                                                "station_id",
                                                "destination_id",
                                                "delay",
                                                "departure_id")

                    depDf = pd.concat(
                        [depDf, cur_depDf], ignore_index=True)

                except KeyError:
                    self.logger.warning(
                        f'KeyError: {station} not found in with' +
                        f'line {dep.label}')
                    # self.logger.error(
                    #     ErrorType="KeyError",
                    #     station=station, line=dep["label"])
                    continue
        return depDf

    def loadDf(self):
        if self.db_connector is None:
            self.get_connector()
        return pd.read_sql_table(self.depTableName, self.db_connector)

    def backup_table(self):
        back_up_file_path = self.backUpFolder.joinpath(f"{self.depTableName}.csv")
        backup_query = f"""
                Copy {self.depTableName} to '{back_up_file_path}' CSV header
                """
        self.db_connector.execute(sa.text(backup_query))

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
        temp_table_name = "temp_departure"
        self.cumDf.to_sql(
            temp_table_name,
            self.db_connector,
            if_exists="replace",
            index=False)
        comma_seperated_columns = ",".join(self.cumDf.columns)
        insert_query = f"""
                insert into {self.depTableName} ({comma_seperated_columns})
                select {comma_seperated_columns}
                from {temp_table_name}
                on conflict (departure_id)
                do nothing
        """

        update_query = f"""
                UPDATE {self.depTableName} AS d
                SET delay = t.delay,
                    time_of_record = t.time_of_record
                FROM {temp_table_name} AS t
                WHERE d.departure_id = t.departure_id
        """
        self.db_connector.execute(sa.text(insert_query))
        self.db_connector.execute(sa.text(update_query))

    def signal_handler(self, signal, frame):
        self.update_db_table()
        self.backup_table()
        self.logger.info("Saving data to backup-dir and shutting down")
        sys.exit(0)

    def clean_up_proc(self):
        self.update_db_table()

    async def get_df_by_chunks(self):
        for i in range(0, len(self.stationID), self.chunk_size):
            chunk = dict(itertools.islice(
                self.stationID.items(), i, i + self.chunk_size))
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
        # self.create_db_table(self.cumDf, self.depTableName)
        # transDf = self.calculate_transfer(self.cumDf)
        # self.create_db_table(transDf, self.transTableName)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        while True:
            try:
                epoch_now = datetime.now()
                currDf = await self.get_df_by_chunks()
                self.cumDf = pd.concat([self.cumDf, currDf])
                self.cumDf = self.cumDf.drop_duplicates(
                    ["departure_id"], keep="last")
                if epoch_now.hour == 3 and epoch_now.minute == 0:
                    self.backup_table()
                    self.logger.info("executing planned db backup")
                    self.last_saved = datetime.today().date()

                epochTime_Df = currDf.time_of_dep.min()
                closest_dep: timedelta = epochTime_Df - epoch_now
                if closest_dep.days <= 0:
                    components = abs(closest_dep).components
                    self.logger.info(
                        "active connection with most recent departure before " +
                        f"{components.hours}:{components.minutes}:{components.seconds}, next " +
                        f"refresh in {self.refreshInterval} seconds")

                if closest_dep > timedelta(days=0, hours=0, minutes=5):
                    inbuild_time.sleep(closest_dep.total_seconds())
                    self.logger.info(
                        f"sleepmode, waiting for {closest_dep}")

                inbuild_time.sleep(self.refreshInterval)

                if (epoch_now % timedelta(minutes=15)).total_seconds() < 10:
                    self.logger.info(
                        "saving snapshot of Departures to database, saving " +
                        f"again in {self.saveInterval // 60} minutes")
                    self.update_db_table()
                    self.cumDf = self.cumDf[0:0]
                    inbuild_time.sleep(30)

            except aiohttp.ServerConnectionError:
                self.logger.error("ResponseError")
                self.update_db_table()
                self.cumDf = self.cumDf[0:0]
                inbuild_time.sleep(1)
                continue

            except aiohttp.TooManyRedirects:
                self.logger.error("TooManyRedirectsError")
                self.update_db_table()
                self.cumDf = self.cumDf[0:0]
                inbuild_time.sleep(120)
                continue

            except AssertionError:
                self.logger.error("AssertionError")
                self.update_db_table()
                self.cumDf = self.cumDf[0:0]
                inbuild_time.sleep(120)
                continue
            except aiohttp.ClientConnectionError:
                self.logger.error("InternetConnectionError")
                self.update_db_table()
                self.cumDf = self.cumDf[0:0]
                inbuild_time.sleep(120)
                continue

            except asyncio.TimeoutError:
                self.logger.error("TimeOutError")
                self.update_db_table()
                self.cumDf = self.cumDf[0:0]
                inbuild_time.sleep(360)
                continue

            except IndexError:
                self.logger.error("PayloadError")
                inbuild_time.sleep(30)
                continue
