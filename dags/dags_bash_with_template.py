from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum


with DAG(
    dag_id="dags_bash_with_template",
    schedule="0 0 * * *",  # 매일 0시 0분에 실행
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command='echo "data_interval_end: {{ data_interval_end }}"',
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
            'START_DATE': '{{data_interval_start | ds}}',
            'END_DATE': '{{data_interval_end | ds}}'
        },
        bash_command='echo "START_DATE: $START_DATE, END_DATE: $END_DATE"',
    )

    bash_t1 >> bash_t2