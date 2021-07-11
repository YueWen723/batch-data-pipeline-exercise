from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def normalize_csv(ts, **kwargs):
    import csv
    source_filename = kwargs['source']
    target_filename = kwargs['target']
    header_skipped = False
    with open(source_filename, newline='') as source_file:
        with open(target_filename, "w", newline='') as target_file:
            reader = csv.reader(source_file, delimiter=',')
            writer = csv.writer(target_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if not header_skipped:
                    header_skipped = True
                    continue
                row.append(ts)
                writer.writerow(row)
    return target_filename

def load_csv_to_postgres(table_name, **kwargs):
    csv_filepath = kwargs['csv_filepath']
    connecion = PostgresHook(postgres_conn_id=connection_id)
    connecion.bulk_load(table_name, csv_filepath)
    return table_name
connection_id = 'dwh'

create_fact_inventory_snapshot_sql="""
DROP TABLE fact_inventory_snapshot;
CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
    productId VARCHAR NOT NULL,
    amount INTEGER,
    date timestamp,
    update_time timestamp
);

truncate fact_inventory_snapshot;
"""

with DAG(
        dag_id="process_inventory",
        start_date=datetime(2020,1,1),
        schedule_interval="@once",
        default_args=default_args,
        catchup=False,
) as dag:
    check_inventory_csv_readiness = BashSensor(
        task_id="check_inventory_csv_readiness",
        bash_command="""
            ls /data/raw/inventory_{{ ds }}.csv
        """,
    )

    normalize_inventory_csv = PythonOperator(
        task_id='normalize_inventory_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/inventory_{{ ds }}.csv",
            'target': "/data/stg/inventory_{{ ds }}.csv"
        },
    )

    create_fact_inventory_snapshot_table = PostgresOperator(
        task_id="create_fact_inventory_snapshot_table",
        postgres_conn_id=connection_id,
        sql=create_fact_inventory_snapshot_sql,
    )

    load_inventory_to_inventory_snapshot_table = PythonOperator(
        task_id="load_inventory_to_inventory_snapshot_table",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "../../data/stg/inventory_{{ ds }}.csv",
            'table_name': 'fact_inventory_snapshot'
        },
    )

check_inventory_csv_readiness >> normalize_inventory_csv >> create_fact_inventory_snapshot_table >> load_inventory_to_inventory_snapshot_table



