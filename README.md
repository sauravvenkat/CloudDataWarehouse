# CloudDataWarehouse
This is an ETL pipeline taking source data from an open source music database stored in Amazon S3, transforming the data, and then finally uploading the data into Amazon Redshift.

## ETL Pipeline and Data Warehouse
This is an ETL pipeline taking csv files from Amazon S3, transforming them, and then uploading them into an Amazon Redshift Data Warehouse.

## Running Scripts:
The scripts should be executed in the below order:

python3 create_tables.py python3 etl.py

## Scripts in Repository:
create_tables.py
This script executes the queries used to create the Amazon Redshift staging and star schema data warehouse tables.

etl.py
This script executes the queries used to bulk insert data from Amazon S3 into staging tables and then finally transform and insert data into the data warehouse.

