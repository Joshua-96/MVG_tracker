import pandas as pd
import pathlib as pl
import sys
from data_gathering import get_json_from_path
from sqlalchemy import create_engine


def main():
    currPath = pl.Path(__file__).parent
    CSV_PATH = pl.Path(sys.argv[1])
    jsonRelPath = "MVG_config.json"
    jsonPath = currPath.joinpath(jsonRelPath)
    config = get_json_from_path(jsonPath)
    conn = create_engine(
        f'postgresql://{config["dbParams"]["user"]}:' +
        f'{config["dbParams"]["password"]}' +
        f'@172.24.2.1:{config["dbParams"]["port"]}',
        pool_recycle=3600)
    conn.connect()
    df = pd.read_csv(str(CSV_PATH), sep=";", index_col=False)
    print(CSV_PATH.stem)
    df.to_sql(CSV_PATH.stem, conn, if_exists="append", index=False)


if __name__ == "__main__":
    main()
