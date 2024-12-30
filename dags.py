from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG Tanımı
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world_dag_3min',
    default_args=default_args,
    description='3 dakikada bir hello world yazan DAG',
    schedule_interval=timedelta(minutes=3),  # 3 dakikada bir çalışacak şekilde ayarlandı
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Görevler (Dağ şekli oluşturacak şekilde)
tasks = []

for i in range(1, 6):
    task = BashOperator(
        task_id=f'echo_task_up_{i}',
        bash_command=f'echo {"hello world " * i}',
        dag=dag,
    )
    tasks.append(task)

for i in range(4, 0, -1):
    task = BashOperator(
        task_id=f'echo_task_down_{i}',
        bash_command=f'echo {"hello world " * i}',
        dag=dag,
    )
    tasks.append(task)

# Görevlerin sıralı bağlanması
for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]
