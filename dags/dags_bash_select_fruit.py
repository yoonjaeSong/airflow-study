import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1",  # 매월 첫째주 토요일 0시 10분에 실행
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    run_apple = BashOperator(
        task_id="run_apple",
        bash_command="/opt/airflow/plugins/select_fruit.sh APPLE",
    )

    run_orange = BashOperator(
        task_id="run_orange",
        bash_command="/opt/airflow/plugins/select_fruit.sh ORANGE",
    )

    run_apple >> run_orange