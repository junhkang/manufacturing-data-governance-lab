from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG 정의
with DAG("hello_airflow",
         start_date=datetime(2024, 4, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Airflow 설치 완료!'"
    )
