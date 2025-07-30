from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

from common.common_func import get_sftp



with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *", 
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    task_get_sftp=PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp
    )
