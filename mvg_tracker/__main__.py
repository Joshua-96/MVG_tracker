import logging
import os
import asyncio
import pathlib as pl
from mvg_tracker.data_gathering import DataManager, get_json_from_path,\
    get_json_from_path


if __name__ == "__main__":
    currPath = pl.Path(__file__).parent
    logging.basicConfig(
        filename=os.path.join(currPath, "traceback.log"),
        filemode='a',
        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S',
        level=logging.ERROR)

    jsonRelPath = "/config/defaultconfig.json"
    jsonPath = os.path.join(currPath, jsonRelPath)
    config = get_json_from_path(jsonPath)
    data_manager = DataManager(config, "MVG1", "MVG_Trans1", currPath)
    #logging.basicConfig(filename="tracebackss.log",encoding="utf-8")
    asyncio.run(data_manager.main(config=config))
