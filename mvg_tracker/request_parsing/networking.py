import json
import logging
import aiohttp
import asyncio
import sys
from mvg_tracker.logging_util.init_loggers import init_console_logger


logger = logging.getLogger("RequestHandeler")
logger = init_console_logger(logger)
logger.setLevel(logging.DEBUG)


async def get_json(client, url):
    async with client.get(url) as response:
        if response.status != 200:
            logger.debug(f"got bad response from server {response.status} at url {url}")
            return
        return await response.read()


async def get_dep(station, client):
    content = await get_json(
        client,
        f"https://www.mvg.de/api/fib/v2/departure?globalId={station}")
    # content=requests.get(URL)
    if content is None:
        return
    querey = json.loads(content.decode())
    return querey


async def cache_dep(stationID, client):
    pending_tasks = [asyncio.ensure_future(
        get_dep(i, client)) for i in list(stationID.values())]
    cachedDep = await asyncio.gather(*pending_tasks)
    return cachedDep


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