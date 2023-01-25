

import argparse
from os.path import exists
from os import getenv
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    #user = params.user
    #pswd = params.pswd
    #host = params.host
    #port = params.port
    #db = params.db
    USER = getenv("USER")
    PSWD = getenv("PSWD")
    HOST = getenv("HOST")
    PORT = getenv("PORT")
    DB = getenv("DB")

    TABLE_NAME = params.table_name
    URL = params.url

    file = URL.split("/")
    file = file[len(file) - 1]
    file = file.split(".")[0]
    FILE_NAME = file+ ".csv"     
    
    url_temp = URL.split(".")
    URL_EXTENSION = url_temp[len(url_temp)-1].lower()
    URL_EXTENSION

    if not exists(FILE_NAME):
        if URL_EXTENSION == "csv":
            ny_data = pd.read_csv(URL)

        elif URL_EXTENSION == "parquet":
            ny_data = pd.read_parquet(URL)

        else:
            raise ValueError # Shortcut as this is a toy script.

        ny_data.to_csv(FILE_NAME, index=False)

    #df = pd.read_csv("yellow_tripdata_2021-01.csv", nrows=100, parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    engine = create_engine(f"postgresql://{USER}:{PSWD}@{HOST}:{PORT}/{DB}")

    # Ref for datetime column check: https://stackoverflow.com/questions/70160896/pandas-use-column-names-if-do-not-exist
    r =  open(FILE_NAME)
    headers = pd.read_csv(r, nrows=0).columns.tolist()
    dt_cols = [i for i in headers if "date" in i.lower() or "time" in i.lower()]
    r.seek(0) # return file pointer to beginning.
    df_iter = pd.read_csv(r,
        #parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
        parse_dates=dt_cols,
        iterator=True,
        chunksize=100000,
        low_memory=False
        )

    #df = next(df_iter)
    #print(pd.io.sql.get_schema(df.head(), name="yellow_taxi_data", con=engine))

    df_init = True
    data_remaining=True
    while data_remaining:
        try:
            df = next(df_iter)
            if df_init:
                df.to_sql(name=TABLE_NAME, con=engine, if_exists="replace")
                df_init = False
            else:
                df.to_sql(name=TABLE_NAME, con=engine, if_exists="append")
        except StopIteration:
            print("All data retrieved.")
            data_remaining=False

    r.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    # Passing as environment variables instead.
    # user, password, host, port, db name, table name, csv url.
    # parser.add_argument("--user", help="user name for postgres")
    # parser.add_argument("--pswd", help="password for postgres")
    # parser.add_argument("--host", help="host for postgres")
    # parser.add_argument("--port", help="port for postgres")
    # parser.add_argument("--db", help="db name for postgres")
    parser.add_argument("--table_name", help="table name for results")
    parser.add_argument("--url", help="url for data source")

    args = parser.parse_args()

    main(args)



