import json

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

import pandas as pd

def _process_data(ti):
    data=ti.xcom_pull(task_ids='extract_data')
    time_series_data = data['Time Series (60min)']

    processed_data = []

    for snapshot_time, metrics in time_series_data.items():
        record = {
            'symbol': 'ACN',
            'open': float(metrics['1. open']),
            'high': float(metrics['2. high']),
            'low': float(metrics['3. low']),
            'close': float(metrics['4. close']),
            'volume': int(metrics['5. volume']),
            'snapshot_time': datetime.strptime(snapshot_time, '%Y-%m-%d %H:%M:%S').isoformat(),
            'load_time': datetime.now().isoformat(),
        }
        processed_data.append(record)

    ti.xcom_push(key='processed_data', value=processed_data)

def _process_data_to_csv(ti):
    processed_data = ti.xcom_pull(task_ids='process_data', key='processed_data')

    today_str = datetime.today().strftime("%Y-%m-%d")
    directory = '/tmp/stocks'
    csv_file_path=f'/tmp/stocks/processed_data_{today_str}.csv'
    df = pd.DataFrame(processed_data)

    os.makedirs(directory, exist_ok=True)

    df.to_csv(csv_file_path, index=None, header=False)

    ti.xcom_push(key='csv_file_path', value=csv_file_path)

def _process_data_to_db(ti):
    csv_file_path = ti.xcom_pull(task_ids='process_data_to_csv', key='csv_file_path')

    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY stocks from stdin WITH DELIMITER as ','",
        filename=csv_file_path
    )


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
        snapshot_time TIMESTAMP NOT NULL,
        load_time TIMESTAMP NOT NULL
        )
        '''
    )

    extract_data = SimpleHttpOperator(
        task_id='extract_data',
        http_conn_id='alphavantage-api',
        method='GET',
        endpoint='query',
        data={
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': 'ACN',
            'interval': '60min',
            'outputsize': 'full',
            'apikey': '{{ var.value.API_KEY }}',
            'month' : '2012-01'
        },
        response_filter=lambda response:json.loads(response.text),
        log_response=True
    )

    process_data=PythonOperator(
        task_id='process_data',
        python_callable=_process_data
    )

    process_data_to_csv=PythonOperator(
        task_id='process_data_to_csv',
        python_callable=_process_data_to_csv
    )

    process_data_to_db = PythonOperator(
        task_id='store_data',
        python_callable=_process_data_to_db
    )

create_table >> extract_data >> process_data >> process_data_to_csv >> process_data_to_db

