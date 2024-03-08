from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime




with DAG('stock_processing', start_date=datetime(2024, 3, 8),
         schedule='@monthly', catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS stocks (
        symbol TEXT NOT NULL,
        open FLOAT NOT NULL,
        high FLOAT NOT NULL,
        low FLOAT NOT NULL,
        close FLOAT NOT NULL,
        volume BIGINT NOT NULL,
        load TIMESTAMP NOT NULL,
        )
        '''
    )