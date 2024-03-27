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