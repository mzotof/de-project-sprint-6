import contextlib
from pathlib import Path

import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

import pandas as pd
import vertica_python


def load_dwh_table_operator(table_name: str):
    def load_dwh_table():
        sql_path = Path(__file__).parent.joinpath('sql')

        vertica_conn_creds = BaseHook.get_connection('dwh')
        vertica_conn = vertica_python.connect(
            host=vertica_conn_creds.host,
            port=vertica_conn_creds.port,
            user=vertica_conn_creds.login,
            password=vertica_conn_creds.password,
        )

        with contextlib.closing(vertica_conn.cursor()) as cur:
            with open(sql_path.joinpath(f'{table_name}.sql')) as f:
                cur.execute(f.read())

        vertica_conn.commit()
        vertica_conn.close()

    return PythonOperator(task_id=table_name, python_callable=load_dwh_table)


@dag(schedule_interval=None, start_date=pendulum.parse('2023-12-25'))
def stg2dwh():
    h_dialogs = load_dwh_table_operator('h_dialogs')
    h_groups = load_dwh_table_operator('h_groups')
    h_users = load_dwh_table_operator('h_users')
    l_admins = load_dwh_table_operator('l_admins')
    l_groups_dialogs = load_dwh_table_operator('l_groups_dialogs')
    l_user_group_activity = load_dwh_table_operator('l_user_group_activity')
    l_user_message = load_dwh_table_operator('l_user_message')
    s_admins = load_dwh_table_operator('s_admins')
    s_auth_history = load_dwh_table_operator('s_auth_history')
    s_dialog_info = load_dwh_table_operator('s_dialog_info')
    s_group_name = load_dwh_table_operator('s_group_name')
    s_group_private_status = load_dwh_table_operator('s_group_private_status')
    s_user_chatinfo = load_dwh_table_operator('s_user_chatinfo')
    s_user_socdem = load_dwh_table_operator('s_user_socdem')

    [h_groups, h_users] >> l_admins >> s_admins
    [h_groups, h_users] >> l_user_group_activity >> s_auth_history
    [h_dialogs, h_groups] >> l_groups_dialogs
    [h_dialogs, h_users] >> l_user_message
    h_dialogs >> s_dialog_info
    h_groups >> [s_group_name, s_group_private_status]
    h_users >> [s_user_chatinfo, s_user_socdem]


_ = stg2dwh()
