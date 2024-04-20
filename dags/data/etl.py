from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

def _get_files(filepath):
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files

def _create_tables():
    pass

def _process():
    pass

with DAG(
    "etl",
    start_date=timezone.datetime(2024,4,1),
    schedule="@daily",
    tag=["swu"],
    ):

    start = EmptyOperator(task_id="start")

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
    )

    create_tables =PythonOperator(
        task_id="process",
        python_callable=,
    )

    process = PythonOperator(
        task_id="process",
        python_callable=,
    )

    end = EmptyOperator(task_id="end")

    start >> [get_files,create_tables] >> process >> end