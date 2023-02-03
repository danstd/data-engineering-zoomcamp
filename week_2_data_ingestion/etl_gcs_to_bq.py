import json
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from pathlib import Path, PurePosixPath

@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dtc-data-lake-de-z-camp")
    gcs_block.get_directory(from_path=gcs_path, local_path=Path(__file__).parent.resolve() / Path(f"data/{color}"))
    #return Path("../"+gcs_path)
    return Path(__file__).parent.resolve() / Path(f"data/{color}")


@task(retries=3, log_prints=True)
def gcs_load(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)

    print(f"Number of records: {len(df)}")
    return df

@task(retries=3, log_prints=True)
def bq_write(color: str, df: pd.DataFrame) -> None:

    gcp_credentials_block = GcpCredentials.load("dsd-de-zoom-camp")

    df.to_gbq(
        destination_table=f"de_z_camp_dataset.{color}_taxi",
        project_id="de-z-camp",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gc_to_bq(color: str="green", year: int=2020, month: int=11):
    """Main ETL flow to load data into Big Query Data Warehouse"""

    path = extract_from_gcs(color=color, year=year, month=month)
    df = gcs_load(path)
    bq_write(color=color, df=df)
    
if __name__ == "__main__":
    etl_gc_to_bq()