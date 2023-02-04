import argparse
import json
from pathlib import Path, PurePosixPath
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def fetch(dataset_url: str, schema: dict, date_cols: list) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    if schema is not None:
        df = pd.read_csv(dataset_url, dtype=schema, parse_dates=date_cols)
    else:
        df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str, output_schema: dict, column_map: dict) -> pd.DataFrame:
    """Fix dtype issues"""

    # Add source-distinguishing column.
    df["color"] = color

    # Rename columns if specified in column_map.
    rename_dict=dict()
    for col, combined_col in column_map.items():
        if col in df.columns:
            rename_dict[col] = combined_col
    if len(rename_dict) > 0:
        df.rename(columns=rename_dict, inplace=True)

    # Add empty column of proper type if in output_schema but not in dataframe.
    for col in output_schema.keys():
        if col not in df.columns:
            df[col] = pd.Series(dtype=output_schema[col])

    #df["passenger_count"].fillna(0, inplace=True)

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    from_path = Path(__file__).parent.resolve() / Path(f"data/{color}/{dataset_file}.parquet")

    to_path = PurePosixPath(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(from_path, compression="gzip")
    return from_path, to_path


@task()
def write_gcs(from_path: Path, to_path: Path) -> None:
    """Upload local parquet file to GCS"""

    gcs_block = GcsBucket.load("dtc-data-lake-de-z-camp")
    gcs_block.upload_from_path(from_path=from_path, to_path=to_path)
    return


@flow()
def etl_web_to_gcs(color: str="green", year: int=2020, month: int=1) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    # Read schema
    with open(Path(__file__).parent.resolve() / "taxi_schemas.json", "r") as f:
        schemas = json.load(f)
    
    date_cols = list()
    try:
        schema = schemas[color]
        combined_schema = schemas["trip_data"]
        column_map = schemas["col_map"]
        for col, dtype in schema.items():
            if dtype == "datetime":
                schema[col] = "str"
                date_cols.append(col)
    except ValueError:
        schema = None

    df = fetch(dataset_url, schema=schema, date_cols=date_cols)
    df_clean = clean(df, color=color, output_schema=combined_schema, column_map=column_map)
    
    from_path, to_path = write_local(df_clean, color, dataset_file)
    write_gcs(from_path=from_path, to_path=to_path)


def main(params):
    color = params.color
    year = params.year
    month = params.month

    etl_web_to_gcs(color=color, year=year, month=month)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--color", help="Taxi color")
    parser.add_argument("--year", help="Year")
    parser.add_argument("--month", help="Month (1-12)")

    parser.set_defaults(color="yellow", year=2019, month=3)
    args = parser.parse_args()

    main(args)
