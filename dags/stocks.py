import json

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def _process_data(ti):
    data=ti.xcom_pull(task_ids='extract_data')
    time_series_data = data['Time Series (60min)']

    processed_data = []

    for snapshot_time, metrics in time_series_data.items():
        record = {
            'symbol': 'ACC',
            'open': float(metrics['1. open']),
            'high': float(metrics['2. high']),
            'low': float(metrics['3. low']),
            'close': float(metrics['4. close']),
            'volume': int(metrics['5. volume']),
            'snapshot_time': datetime.strptime(snapshot_time, '%Y-%m-%d %H:%M:%S'),
            'load_time': datetime.now(),
        }
        processed_data.append(record)

    ti.xcom_push(key='processed_data', value=process_data)


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
        load_time TIMESTAMP NOT NULL,
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
            'symbol': 'ACC',
            'interval': '60min',
            'outputsize': 'full',
            'apikey': '{{ var.value.API_KEY }}'
        },
        response_filter=lambda response:json.loads(response.text),
        log_response=True
    )

    process_data=PythonOperator(
        task_id='process_user',
        python_callable=_process_data
    )

