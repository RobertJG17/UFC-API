from airflow.models import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from scripts import fighters_scrape, stats_scrape, data_merge


args = {
    'owner': 'Robert Guerra',
    'start_date': days_ago(1)
}


with DAG(
    dag_id='web-scrape-pipeline-dag', 
    default_args=args, 
    schedule_interval="0 0 * * 0,4",
    ) as python_dag:


    fighters_scrape_task = PythonOperator(
        task_id='python_dag_task_1', 
        python_callable=fighters_scrape.fighters_entrypoint, 
        dag=python_dag
    )

    stats_scrape_task = PythonOperator(
        task_id='python_dag_task_2', 
        python_callable=stats_scrape.stats_entrypoint, 
        dag=python_dag
    )

    data_merge_task = PythonOperator(
        task_id='python_dag_task_3', 
        python_callable=data_merge.data_merge_entrypoint, 
        dag=python_dag
    )

    fighters_scrape_task >> stats_scrape_task >> data_merge_task
