from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


def print_a():
    print("hi from task a")

def print_b():
    print("hi from task b")


with DAG(
    "my_dag",
    description="A first simple dag to start",
    tags=["dev", "data_engineering"],
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task_a = PythonOperator(task_id="task_a", python_callable=print_a)
    task_b = PythonOperator(task_id="task_b", python_callable=print_b)

    task_a >> task_b
