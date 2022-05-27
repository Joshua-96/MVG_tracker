import time
from datetime import datetime
from numpy import int64
import pandas as pd
import json
import pathlib as pl
import asyncio
import aiohttp
import sys
import logging
import os
from dataclasses import dataclass, fields
import sqlalchemy as sa
from utils import init_console_logger, init_file_logger

# from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger("RequestHandeler")
logger = init_console_logger(logger)

@dataclass
class departure:
    timestamp_epoch: int
    timestamp: datetime
    station: str
    line: str
    destination: str
    delay: int
    Id: str
    curr_time_epoch: int64


async def get_json(client, url):
    async with client.get(url) as response:
        if response.status != 200:
            logger.info(f"got bad response from server{response.status}")
            raise aiohttp.ServerConnectionError
        # assert response.status == 200
        return await response.read()


async def get_dep(station, client):
    content = await get_json(client, f"https://www.mvg.de/api//fahrinfo/departure/{station}?footway=0")
    # content=requests.get(URL)
    querey = json.loads(content.decode())
    return querey


async def cache_dep(stationID, client):
    pending_tasks = [asyncio.ensure_future(
        get_dep(i, client)) for i in list(stationID.values())]
    cachedDep = await asyncio.gather(*pending_tasks)
    return cachedDep


def signal_handler(signal, frame):
    loop.stop()
    client.close()
    sys.exit(0)


def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


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
    merged.drop(f"timestamp_epoch{suffixes[0]}", axis="columns", inplace=True)
    if f"timestamp_epoch{suffixes[1]}" in list(merged.columns):
        merged.drop(
            f"timestamp_epoch{suffixes[1]}", axis="columns", inplace=True)
    # merged.sort_values(by=[f"timestamp{suffixes[0]}",f"timestamp{suffixes[1]}"],inplace=True)
    # merged.drop_duplicates([f"timestamp{suffixes[0]}","station",f"line{suffixes[0]}",f"line{suffixes[1]}"],keep="first",inplace=True)

    return merged


class Logger:
    def __init__(self, path, FileType, LogLevel=0):
        self.path = path
        self.FileType = FileType
        self.LogLevel = LogLevel

    def error(self, ErrorType: str, station="", line=""):
        dateStr = datetime.today().strftime('%Y-%m-%d')
        FilePath = self.path.joinpath(dateStr).with_suffix(f".{self.FileType}")
        timeOfDay = datetime.today().strftime('%H:%M:%S')
        Entry = {"ErrorType": [ErrorType], "time": [
            timeOfDay], "station": [station], "line": [line]}
        df = pd.DataFrame(Entry)
        if self.FileType == "csv":
            if os.path.isfile(FilePath):
                df.to_csv(FilePath, ";", index=False, mode="a", header=False)
            else:
                df.to_csv(FilePath, ";", index=False, mode="w")
        else:
            raise NotImplementedError("Only csv Files supported")

    def write_file(self, e: Exception):
        pass


class DataManager:

    def __init__(self, config, depTableName, transTableName, loggingDir):
        self.dbParams = config["dbParams"]
        self.replaceMap = config["replaceMap"]
        self.assistFiles = config["assistFiles"]
        self.depTableName = depTableName
        self.transTableName = transTableName
        self.refreshInterval = 30
        self.saveInterval = 960
        self.db_connector = None
        self.cwd = str(pl.Path(__file__).parent)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger = init_console_logger(self.logger, logging.DEBUG)
        self.logger = init_file_logger(self.logger, loggingDir, logging.INFO)
        # self.logger = Logger(self.loggingDir, "csv")
        self.timeOffset = 2*60*3600
        self.load_trans_config()

    def init_stationID(self):
        df = self.FileDict["stationFile"][["station", "ID"]]
        stations = df.station.to_list()
        keys = df.ID.to_list()
        self.stationID = dict(zip(stations, keys))

    def get_Df(self, cachedDep: dict, stationID: dict):
        depDf = pd.DataFrame(
            columns=[field.name for field in fields(departure)])
        for i, station in enumerate(stationID):
            querey = cachedDep[i]
            for dep in querey["departures"]:
                try:
                    if dep["product"] == "SBAHN"\
                            and (int(dep["departureTime"]/1000) > int(time.time()) - 3600)\
                            and "delay" in dep.keys():
                        timestamp_epoch = int(
                            dep["departureTime"]/1000) + int(dep["delay"]*60)
                        cur_dep = departure(
                            timestamp_epoch=timestamp_epoch,
                            timestamp=datetime.fromtimestamp(
                                timestamp=timestamp_epoch).strftime("%Y/%m/%d, %H:%M:%S"),
                            station=station,
                            line=dep["label"],
                            destination=dep["destination"],
                            delay=dep["delay"],
                            Id=dep["departureId"],
                            curr_time_epoch=int(
                                datetime.timestamp(datetime.now()))
                        )
                        cur_depDf = pd.DataFrame(data=[cur_dep])
                        depDf = pd.concat(
                            [depDf, cur_depDf], ignore_index=True)

                except KeyError:
                    self.logger.warning(
                        f'KeyError: {station} not found in with line {dep["label"]}')
                    # self.logger.error(
                    #     ErrorType="KeyError",
                    #     station=station, line=dep["label"])
                    continue
        return depDf

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

    def load_trans_config(self):
        self.FileDict = {i: None for i in self.assistFiles}
        for name in self.assistFiles:
            loadPath = os.path.join(self.cwd, self.assistFiles[name])
            self.FileDict[name] = pd.read_csv(
                loadPath,
                sep=";",
                index_col=None,
                encoding="cp1252")
        self.init_stationID()
        self.FileDict["stationFile"].drop(["Breitengrad", "Längengrad", "ID"],
                                          axis="columns",
                                          inplace=True)

    def loadDf(self):
        if self.db_connector is None:
            self.get_connector()
        return pd.read_sql_table(self.depTableName, self.db_connector)

    def get_connector(self):
        srv = "postgresql"
        user = self.dbParams["user"]
        pw = self.dbParams["password"]
        host = self.dbParams["host"]
        port = self.dbParams["port"]
        db = self.dbParams["database"]
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
        self.db_connector = alchemyEngine.connect()

    def create_db_table(self, Df, tableName):
        if self.db_connector is None:
            self.get_connector()
        Df.to_sql(tableName, self.db_connector, if_exists="append", index=False)

    def update_db_table(self, Df):
        if self.db_connector is None:
            self.get_connector()
        time_thresh = time.time()-self.timeOffset
        baseDf = pd.read_sql(
            f'SELECT * FROM public."{self.depTableName}" WHERE timestamp_epoch >= {time_thresh}',
            self.db_connector)
        self.db_connector.execute(
            f'DELETE FROM public."{self.depTableName}" WHERE timestamp_epoch >= {time_thresh}')
        baseDf = pd.concat([baseDf, Df], ignore_index=True)
        baseDf.drop_duplicates("Id", inplace=True, keep="last")
        transTable = self.calculate_transfer(baseDf)
        baseTransDf = pd.read_sql(
            f'SELECT * FROM public."{self.transTableName}" WHERE timestamp_epoch >= {time_thresh}',
            self.db_connector)
        baseTransDf = pd.concat([baseTransDf, transTable], ignore_index=True)
        baseTransDf.drop_duplicates(
            ["timestamp_from", "station", "line_from", "line_to"],
            ignore_index=True,
            inplace=True)
        self.db_connector.execute(
            f'DELETE FROM public."{self.transTableName}" WHERE timestamp_epoch >= {time_thresh}')
        baseTransDf.to_sql(self.transTableName,
                           self.db_connector,
                           if_exists="append",
                           index=False)
        baseDf.to_sql(self.depTableName,
                      self.db_connector,
                      if_exists="append",
                      index=False)

    async def main(self, config):
        if config is None:
            raise ValueError("No Config given")

        loop = loop = asyncio.get_event_loop()
        client = aiohttp.ClientSession(loop=loop)

        cachedDep = await cache_dep(self.stationID, client)
        cumDf = self.get_Df(cachedDep, self.stationID)
        self.create_db_table(cumDf, self.depTableName)
        transDf = self.calculate_transfer(cumDf)
        self.create_db_table(transDf, self.transTableName)

        while True:
            try:
                epoch_now = int(datetime.timestamp(datetime.now()))
                cachedDep = await cache_dep(self.stationID, client)
                currDf = self.get_Df(cachedDep, self.stationID)
                cumDf = pd.concat([cumDf, currDf])
                cumDf = cumDf.drop_duplicates("Id", keep="last")

                epochTime_Df = int(
                    datetime.timestamp(
                        datetime.strptime(
                            currDf.sort_values(
                                "timestamp")[0:1].timestamp.values[0], "%Y/%m/%d, %H:%M:%S")
                    )
                )

                self.logger.debug(f"active connection with upcomming departure in {epochTime_Df - epoch_now + 30} seconds, next refresh in {self.refreshInterval} seconds")
                # sys.stdout.write(f"working {epochTime_Df - epoch_now} \r")

                if epochTime_Df > (epoch_now + 60):                        
                    time2wait = (epochTime_Df - epoch_now) - 30
                    time.sleep(time2wait)
                    self.logger.info(f"sleepmode, waiting for {time2wait} seconds")
                
                time.sleep(self.refreshInterval)

                if epoch_now % self.saveInterval < self.refreshInterval+2:
                    #print("_________________saving______________________")
                    self.logger.info(f"saving snapshot of Departures to database, saving again in {self.saveInterval // 60} minutes")
                    self.update_db_table(cumDf)
                    cumDf = cumDf[0:0]
                    time.sleep(30)

            except aiohttp.ServerConnectionError:
                print("ResponseError")
                logging.exception("ResponseError")
                self.logger.error("ResponseError")
                self.update_db_table(cumDf)
                cumDf = cumDf[0:0]
                time.sleep(1)
                continue

            except AssertionError:
                #print("___________________AssertionError________________")
                self.logger.error("AssertionError")
                logging.exception("AssertionError")
                self.update_db_table(cumDf)
                cumDf = cumDf[0:0]
                time.sleep(120)
                continue
            except aiohttp.ClientConnectionError:
                #print("______________InternetConnectionError___________")
                self.logger.error("InternetConnectionError")
                self.update_db_table(cumDf)
                cumDf = cumDf[0:0]
                time.sleep(120)
                continue
            except KeyboardInterrupt:
                #print("____________break_________________")
                self.logger.error("KeyboardInterrupt")
                self.update_db_table(cumDf)
                sys.exit(0)
            except IndexError:
                #print("___________________PayloadEmtyError________________")
                self.logger.error("PayloadError")
                time.sleep(30)
                continue
            # except:
            #     print("other exception occured")
            #     logging.exception("something else")
            #     self.update_db_table(cumDf)
            #     sys.exit(1)


def get_json_from_path(Path:pl.Path):

    assert Path.suffix == ".json", \
        f"Incorrect Filetype given: {Path.suffix} expected json"

    if Path.exists():
        with open(Path, "r") as read_file:
            params = json.load(read_file)
    else:
        raise FileNotFoundError(f"at path {Path}")

    return params
