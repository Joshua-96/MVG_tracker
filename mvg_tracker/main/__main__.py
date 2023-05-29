import argparse
import logging
import os
import asyncio
import pathlib as pl
from mvg_tracker.request_parsing.data_gathering import DataManager
from mvg_tracker.data_validation.utils import get_json_from_path
from mvg_tracker.logging_util.init_loggers import init_console_logger, init_file_logger


def main():
    currPath = pl.Path(__file__).parent
    # logging.basicConfig(
    #     filename=os.path.join(currPath, "traceback.log"),
    #     filemode='a',
    #     format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    #     datefmt='%H:%M:%S',
    #     level=logging.ERROR)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log_dir",
        "-l",
        type=str,
        dest="logDir",
        help="enter Dir for logging",
        default="."
    )
    parser.add_argument(
        "--config_path",
        "-c",
        type=str,
        dest="configPath",
        help="enter filePath for the config file to use",
        default=None
    )
    parser.add_argument(
        "--back_up",
        "-b",
        type=str,
        dest="backUpPath",
        help="enter Dir for the backed up files",
        default=None
    )

    args = parser.parse_args()
    logger = logging.getLogger(__name__)
    logger = init_console_logger(logger)
    logger.setLevel(logging.INFO)

    if args.backUpPath is None:
        backUpFolder = pl.Path(
            os.path.expanduser("~")).joinpath(
                "AppData/Roaming/MVG_Tracker/daily")
        logger.warning(
            f"No backup Dir given saving to and loading from {backUpFolder}")
    else:
        backUpFolder = args.backUpPath

    if args.logDir != ".":
        logDir = pl.Path(args.logDir)
        if not logDir.exists():
            raise FileNotFoundError(
                "Dir doesn't exist, please enter valid Path")
    else:
        logger.warning("No logging dir given fallback to cwd")
        logDir = currPath
    if args.configPath is None:
        logger.warning("No config dir given fallback to default config")
        configRelPath = pl.Path("../config/default_config.json")
        configPath = currPath.joinpath(configRelPath)
        configPath.resolve()
    else:
        configPath = pl.Path(args.configPath)
    logger = init_file_logger(logger, str(logDir))
    logger.info(f"start tracking, writing logs to {logDir}," +
                f"getting config from {configPath}")
    config = get_json_from_path(configPath)
    data_manager = DataManager(config=config,
                               loggingDir=logDir,
                               backUpFolder=backUpFolder)

    asyncio.run(data_manager.main(config=config))


if __name__ == "__main__":
    main()
