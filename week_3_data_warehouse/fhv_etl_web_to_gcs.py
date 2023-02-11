import argparse
import json
from pathlib import Path, PurePosixPath
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from datetime import timedelta
import wget


@task(retries=0, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=3))
def fetch_local_write(dataset_url: str, dataset_name: str) -> Path:
    """Retrieve file from URL and store locally"""
    from_path = Path(__file__).parent.resolve() / f"data/fhv/{dataset_name}"

    # Download file
    wget.download(dataset_url, out=str(from_path) )

    to_path = PurePosixPath(f"data/fhv/{dataset_name}")

    return from_path, to_path


@task()
def write_gcs(from_path: Path, to_path: Path) -> None:
    """Upload local file to GCS"""

    gcs_block = GcsBucket.load("dsd-data-zoom-camp-bucket")
    gcs_block.upload_from_path(from_path=from_path, to_path=to_path)
    return


@flow()
def etl_web_to_gcs(year: int=2019, month: int=1) -> None:
    """The main ETL function"""

    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz
    dataset_name = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/" + dataset_name


    from_path, to_path = fetch_local_write(dataset_url=dataset_url, dataset_name=dataset_name)

    write_gcs(from_path=from_path, to_path=to_path)


@flow()
def parent_flow(year: int=2019, months: list[int]=[1]) -> None:

    for month in months:
        etl_web_to_gcs(year=year, month=month)


def main(params):
    year = params.year
    months = params.months

    parent_flow(year=year, months=months)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull FHV data, load to google cloud storage")

    parser.add_argument("--year", help="Year")
    parser.add_argument("--months", nargs='+', help="Months (1-12)")

    parser.set_defaults(year=2019, months=[2,3,4,5,6,7,8,9,10,11,12])
    args = parser.parse_args()

    main(args)
