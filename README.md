# Assignment_ACC_DE


Data from Alphavintage - Monthly timeseries for the ACN stock with 60 mins interval (used free api token stored in the Airflow Variables)
<br>
DAG scheduled monthly, starting with 2023-03-01 with the catchup feature for the bonus task
<br>

Created 5 Operators:
<br>
create_table - creating table stocks in the PostgreSQL DB
<br>
extract_data - calling the API with the defined parameters in order to fetch the Data (month could be configurable for the dag run in the Airflow)
<br>
process_data - processing data from the api
<br>
process_data_to_csv - dumping data into the csv file
<br>
process_data_to_db - inserting data into the db
