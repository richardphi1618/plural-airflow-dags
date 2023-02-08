from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "nobody",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}
dag = DAG(
    "foobar",
    default_args=default_args,
    description="this is a test",
    schedule_interval=None,
    start_date=days_ago(0),
    catchup=False,
    tags=["test", "failure"],
    params={"divisor": 0},
)

t0 = BashOperator(
    task_id="whereami",
    bash_command="pwd",
    dag=dag,
)

t1 = BashOperator(
    task_id="bonk",
    bash_command="echo $((1/{{ params.divisor }}))",
    dag=dag,
)

t0 >> t1
