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