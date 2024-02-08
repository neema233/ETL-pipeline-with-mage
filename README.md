# homework green_taxi_etl pipeline
Data loader block :
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

months = [10, 11, 12]
@data_loader
def load_data():
    """Loads taxi trip data for specified months."""
    dataframes = []
    for month in months:
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz"
        df = pd.read_csv(url, compression="gzip", parse_dates=["lpep_pickup_datetime"])
        dataframes.append(df)
    return pd.concat(dataframes)

treansformer block :
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df):
  # transformations
 df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)] 
 df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
 df = df.rename(columns=lambda x: x.replace("_","").lower())
 return df
def check_data(df):
  # assertions
   assert len(df['VendorID'].unique()) > 0, "No valid vendor IDs found!"
   assert df["passenger_count"].min() > 0, "Minimum passenger count must be greater than 0!"
   assert df["trip_distance"].min() > 0, "Minimum trip distance must be greater than 0!"

data exporter block:
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'ny_taxi'  # Specify the name of the schema to export data to
    table_name = 'green_cab_data'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

  



