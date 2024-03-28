# Green_Taxi_ETL: Mage.ai Pipeline for Green Taxi Data
This project implements a data ETL pipeline using Mage.ai to process green taxi trip data for the final quarter of 2020 (October, November, December).
# Key Features:

- Modular and reusable code blocks with clear functionalities.
- Leverages decorators for data loading, transformation, and exporting.
- Employs unit tests to ensure data quality after transformations.
- Utilizes configuration files for database connection details (separate file).
# Benefits:

- Streamlines data processing for green taxi trip data, enabling efficient analysis.
- Provides a clean and maintainable codebase using Mage.ai features.
- Improves data quality through filtering and assertions.
- Enables flexible configuration for database connection and table details.
# Getting Started:

1.  Ensure you have Mage.ai installed and configured. Refer to the official documentation for installation instructions: https://docs.mage.ai/introduction/overview
2.  Create a configuration file (```io_config.yaml```) containing your specific database connection details (not included in this repository for security reasons).
3.  Execute the pipeline using Mage.ai commands. Refer to the Mage.ai documentation for available commands.

***The pipeline consists of three key blocks:
Data loader,Transformer,Exporter*** 

***codes here*** [codes](https://github.com/neema233/ETL-pipeline-with-mage/tree/main/codes)
# 1. Data Loader Block
- Leverages Pandas to read compressed CSV files (.csv.gz) from a specified URL for each month.
- Iterates through a list of target months (October, November, December) to download and load data.
- Parses the ```lpep_pickup_datetime``` column as datetime format during data loading.
- Concatenates the dataframes from each month into a single dataframe.
# 2. Transformer Block
- Filters out records with zero passengers or zero trip distance.
- Creates a new column ```lpep_pickup_date``` by extracting the date from ```lpep_pickup_datetime```.
-Renames columns from snake_case to lowercase with underscores removed (e.g., ```vendorID``` becomes ```vendor_id```).
- Includes unit tests (using ```@test``` decorator) to verify data integrity after transformation.
     - Ensures there are valid vendor IDs present.
     - Asserts that minimum passenger count and trip distance are greater than zero.

# 3. Expoter Block

- Utilizes Mage.ai's Postgres exporter to write the transformed data to a PostgreSQL database.
- Configuration for the database connection and table details is expected to be defined in a separate ```io_config.yaml``` file.
- Exports the data to a specified schema (```ny_taxi```) and table (```green_cab_data```).
- Handles existing table scenarios by replacing the data (```if_exists='replace'```). 

  



