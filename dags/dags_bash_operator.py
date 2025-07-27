import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",  # 매일 0시 0분에 실행
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    run_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    run_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    run_t1 >> run_t2