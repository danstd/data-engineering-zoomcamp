import json
import os
from os.path import realpath, dirname
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from pathlib import Path, PurePosixPath

@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = PurePosixPath(f"data/{color}/")
    file_name = f"{color}_tripdata_{year}-{month:02}.parquet"
    
    gcs_block = GcsBucket.load("dtc-data-lake-de-z-camp")

    local_dir = Path(dirname(realpath(__file__)))

    gcs_block.get_directory(from_path=gcs_path / file_name, local_path=local_dir)

    local_path = local_dir / Path(f"data/{color}/{file_name}")

    print(local_path)
    return local_path


@task(retries=3, log_prints=True)
def gcs_load(path: Path) -> pd.DataFrame:
    df = None
    df = pd.read_parquet(path)

    print(f"Number of records: {len(df)}")
    return df

@task(retries=3, log_prints=True)
def bq_write(df: pd.DataFrame) -> None:

    gcp_credentials_block = GcpCredentials.load("dsd-de-zoom-camp")

    df.to_gbq(
        destination_table=f"de_z_camp_dataset.trip_data",
        project_id="de-z-camp",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gc_to_bq(color: str="green", year: int=2020, month: int=11):
    """Main ETL flow to load data into Big Query Data Warehouse"""
    df=None
    path = extract_from_gcs(color=color, year=year, month=month)
    df = gcs_load(path)
    bq_write(df=df)
    
if __name__ == "__main__":
    COLOR = "yellow"
    YEAR = 2019
    months = [2,3]
    for month in months:
        etl_gc_to_bq(color=COLOR, year=YEAR, month=month)