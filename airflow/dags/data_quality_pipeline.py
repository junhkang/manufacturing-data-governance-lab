from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import pandas as pd
import shutil
import os

def validate():
    input_path = "/opt/airflow/dags/data/airflow_dag_sample_input.csv"
    output_passed = "/opt/airflow/dags/data/passed.csv"
    output_failed = "/opt/airflow/dags/data/failed.csv"

    df = pd.read_csv(input_path)
    df["TEMP_VALID"] = df["TEMP_C"].between(-50, 200)
    df["QUALITY_VALID"] = df["QUALITY_SCORE"].between(0, 100)
    df["PASS"] = df["TEMP_VALID"] & df["QUALITY_VALID"]

    df[df["PASS"]].to_csv(output_passed, index=False)
    df[~df["PASS"]].to_csv(output_failed, index=False)

    print(f"✔ 총 {len(df)}건 중 {df['PASS'].sum()}건 PASS, {(~df['PASS']).sum()}건 FAIL")

def has_failed_file():
    try:
        df = pd.read_csv("/opt/airflow/dags/data/failed.csv")
        return not df.empty
    except:
        return False

def alert_failed():
    print("🚨 FAIL 데이터가 존재합니다. 담당자 확인 필요!")
    raise Exception("데이터 품질 FAIL 항목 존재")

def archive_passed():
    src = "/opt/airflow/dags/data/passed.csv"
    dst_dir = "/opt/airflow/dags/data/archive"
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, "passed_backup.csv")
    shutil.copy(src, dst)
    print("✅ PASS 데이터 백업 완료")

# --------------------
# DAG 정의
# --------------------
with DAG(
    dag_id="data_quality_pipeline",
    start_date=datetime(2024, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["data-quality", "manufacturing"]
) as dag:

    extract = BashOperator(
        task_id="extract_data",
        bash_command="echo '📦 제조 설비 데이터 불러오기 완료'"
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate
    )

    store_passed = BashOperator(
        task_id="store_passed_data",
        bash_command="cat /opt/airflow/dags/data/passed.csv || echo '파일 없음'"
    )

    store_failed = BashOperator(
        task_id="store_failed_data",
        bash_command="cat /opt/airflow/dags/data/failed.csv || echo '파일 없음'"
    )

    archive = PythonOperator(
        task_id="archive_passed_data",
        python_callable=archive_passed
    )

    check_fail = ShortCircuitOperator(
        task_id="check_failed_data_exists",
        python_callable=has_failed_file
    )

    notify_fail = PythonOperator(
        task_id="notify_failed_data",
        python_callable=alert_failed
    )

    # DAG 흐름 정의
    extract >> validate_task
    validate_task >> [store_passed >> archive, store_failed >> check_fail >> notify_fail]
