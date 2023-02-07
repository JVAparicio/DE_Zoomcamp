from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
#import datetime as dt

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe
        data location = "https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv"
    """

    print(dataset_url)
    df = pd.read_csv(dataset_url, compression='gzip')
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {df.shape[0]}")

    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    "Write DataFrame out locally as parquet file"
    output_dir = Path(f"data/")
    output_dir.mkdir(parents=True, exist_ok=True)

    path = Path(f"data/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")

    #path = Path(f"data/{color}")
    #df.to_parquet(path, compression="gzip", partition_cols=["year", "month","day"])
    return path
 
@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(months: list[int] = [1,2], year: int = 2021):

    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == '__main__':
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10 , 11 ,12]
    year = 2019

    etl_parent_flow(months, year)