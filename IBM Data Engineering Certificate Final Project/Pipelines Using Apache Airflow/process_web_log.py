from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

#arguments/dag

default_args={
    'owner': 'airflow',
    'start_date': datetime.now(),
    'email': 'airflow@example.com'
}

dag = DAG(
    dag_id='process_web_log', 
    schedule='00 12 * * *', #launch everyday at noon
    default_args=default_args, 
    description='Apache Airflow Capstone Project'
)

dag

#tasks
#should extract the ipaddress field from the web server log file
extract_data = BashOperator(
    task_id='extract_data',
    bash_command=(
        "grep -Eo '\\b([0-9]{1,3}\\.){3}[0-9]{1,3}\\b' "
        "/home/project/airflow/dags/capstone/accesslog.txt > "
        "/tmp/extracted_data.csv"
    ),
    dag=dag,
)

#filter out all the occurrences of ipaddress “198.46.149.143” from extracted_data.txt
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=(
        "grep -v '198.46.149.143' " 
        "/tmp/extracted_data.csv > "
        "/tmp/transformed_data.csv"
    ),
    dag=dag
)

#should archive the file transformed_data.txt into a tar file named weblog.tar
load_data = BashOperator(
    task_id="load_data",
    bash_command=(
        "tar -cvf /tmp/weblog.tgz /tmp/transformed_data.csv"
    ),
    dag=dag
)

#pipeline
first_task=extract_data
second_task=transform_data
third_task=load_data

first_task >> second_task >> third_task
