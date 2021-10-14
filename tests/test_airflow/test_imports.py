#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# def test_airflow_imports():
#     from airflow import DAG, settings
#     from airflow.hooks.base import BaseHook
#     from airflow.hooks.dbapi import DbApiHook
#     from airflow.models import DagBag, Connection
#     from airflow.models.baseoperator import BaseOperator
#     from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
#     from airflow.providers.docker.operators.docker import DockerOperator
#     from airflow.providers.postgres.hooks.postgres import PostgresHook
#     from airflow.utils.dates import days_ago

#     BaseHook()
#     PostgresHook()
#     session = settings.Session()
#     conns = session.query(Connection.conn_id).all()

#     def add(x, y):
#         return x + y

#     with DAG("dag", start_date=days_ago(1)) as dag:

#         po_1 = PythonOperator(task_id="add", python_callable=add, op_args=(1, 2))
#         po_2 = PythonVirtualenvOperator(
#             task_id="add_venv", python_callable=add, op_args=(1, 2)
#         )
