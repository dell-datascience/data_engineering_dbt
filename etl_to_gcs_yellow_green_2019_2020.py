from matplotlib.textpath import text_to_path
import pandas as pd
from prefect import task, flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket, cloud_storage_upload_blob_from_string
from pathlib import Path
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

from yaml import serialize

@task(name="downloads data from url for homework", log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract(data_url : str, color: str)-> pd.DataFrame:
    """  
    Download data from url and return a dataframe.

    :param df: pd.DataFrame 
    :return df: pd.DataFrame
    """

    if color == 'green':
        schema_dict = {
                        "VendorID": "Int64",
                        "passenger_count": "Int64",
                        "trip_distance": "float64",
                        "trip_type": "Int64",
                        "RatecodeID": "Int64",
                        "store_and_fwd_flag": "object",
                        "PULocationID": "Int64",
                        "DOLocationID": "Int64",
                        "payment_type": "Int64",
                        "fare_amount": "float64",
                        "extra": "float64",
                        "mta_tax": "float64",
                        "tip_amount": "float64",
                        "tolls_amount": "float64",
                        "improvement_surcharge": "float64",
                        "total_amount": "float64",
                        "congestion_surcharge": "float64"
                        }
    else :
        schema_dict = {
                        "VendorID": "Int64",
                        "passenger_count": "Int64",
                        "RatecodeID": "Int64",
                        "store_and_fwd_flag": "object",
                        "PULocationID": "Int64",
                        "DOLocationID": "Int64",
                        "payment_type": "Int64",
                        "fare_amount": "float64",
                        "extra": "float64",
                        "mta_tax": "float64",
                        "tip_amount": "float64",
                        "tolls_amount": "float64",
                        "improvement_surcharge": "float64",
                        "total_amount": "float64"
                        }
    df: pd.DataFrame = pd.read_csv(data_url).astype(schema_dict, errors="ignore") #,parse_dates=[["tpep_pickup_datetime"],"tpep_dropoff_datetime"])
    return df

@task(name="transformer for data", log_prints=True)
def transform(df: pd.DataFrame)-> pd.DataFrame:
    """
    Transform/remove 0 passenger counts"

    :param df: pd.Dataframe 
    :return df: pd.DataFrame
    """
    
    print(f"\n*** Pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"\n*** Post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(name="loader for data",log_prints=True,) # set to True so that the result is logged in Prefect Cloud
def write_to_local(df:pd.DataFrame, path: Path)->None:
    """  
    Persist the transformed dataset to local

    :param df: Dataframe 
    :return None: None
    """

    df.to_csv(path, compression="gzip")
    return path

@task(name="loader",log_prints=True,) # set to True so that the result is logged in Prefect Cloud
def load(df:pd.DataFrame, path: Path)->None:
    """  
    Load the transformed dataset to Gsc Bucket

    :param df: Dataframe 
    :return None: None
    """

    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(from_path=path,
                               to_path=path)
    return 

@flow(name="main etl", log_prints=True)
def main_flow(month: int, year: int, color: str) ->None:
    """  
    Main ETL pipeline

    :return None: None
    """

    data_file: str = f"{color}_tripdata_{year}-{month:02}" 
    data_url: str = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{data_file}.csv.gz"
    # data_file: str = f"fhv_tripdata_{year}-{month:02}"
    # data_url:  str = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{data_file}.csv.gz"
    # os.makedirs(Path(f"new_data/fhv/"), exist_ok=True)
    os.makedirs(Path(f"new_data/{color}/"), exist_ok=True)
    path = Path(f"new_data/{data_file}.gz")   

    df: df.DataFrame = extract(data_url, color)
    # df: df.DataFrame = transform(df)
    path = write_to_local(df,path)
    load(df, path)
    return None

@flow(name="github_gcs_dbt_etl", log_prints=True)
def github_gcs_dbt_etl(
    months: list[int], years: list[int], colors: list[str])-> None:
    """  
    Run the main flow for a list of colors, months, months

    :return None: None
    """
    for color in colors:
        for year in years:
            for month in months:
                main_flow(month=month,year=year, color=color)
    return None

if __name__=="__main__":
    
    colors = ["yellow","green"]
    years = [2019,2020]
    months: list[int] = [1,2,3,4,5,6,7,8,9,10,11,12]
    github_gcs_dbt_etl(months=months,years=years, colors=colors)