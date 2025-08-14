from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    bash_push = BashOperator(
        task_id="bash_push",
        bash_command="echo START && "
        "echo XCOM PUSH {{ ti.xcom_push(key='bash_pushed', value='first_bash_message') }} && "
        "echo COMPLETE",
    )

    bash_pull = BashOperator(
        task_id="bash_pull",
        env={
            "PUSHED_VALUE": "{{ti.xcom_pull(key='bash_pushed', task_ids='bash_push')}}",
            "RETURN_VALUE": "{{ti.xcom_pull(task_ids='bash_push')}}",
        },
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
        do_xcom_push=False,
    )

    bash_push >> bash_pull
