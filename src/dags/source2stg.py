import contextlib

import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

import pandas as pd
import vertica_python


def load_source2stg(file_name: str):
    dataset_path = f'/data/{file_name}.csv'

    s3_conn = S3Hook('source').get_conn()
    s3_conn.download_file(
        Bucket='sprint6',
        Key=f'{file_name}.csv',
        Filename=dataset_path
    )
    s3_conn.close()

    vertica_conn_creds = BaseHook.get_connection('dwh')
    vertica_conn = vertica_python.connect(
        host=vertica_conn_creds.host,
        port=vertica_conn_creds.port,
        user=vertica_conn_creds.login,
        password=vertica_conn_creds.password,
    )

    with contextlib.closing(vertica_conn.cursor()) as cur:
        cur.execute(f'truncate table stv2023121143__staging.{file_name}')

    df = pd.read_csv(dataset_path)
    num_rows = len(df)
    columns = ', '.join(list(df.columns))
    copy_expr = f"""
    COPY stv2023121143__staging.{file_name} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()


@dag(schedule_interval=None, start_date=pendulum.parse('2023-12-25'))
def source2stg():
    bucket_files = ['users', 'groups', 'dialogs', 'group_log']
    for file_name in bucket_files:
        PythonOperator(
            task_id=file_name,
            python_callable=load_source2stg,
            op_args=[file_name],
        )


_ = source2stg()
