## DAG to creatre EMR cluster with Livy 
## Submits the jobs to EMR cluster
## Terminates the EMR cluster when all the spark jobs are complete
import airflowlib.emr_lib as emr
import os
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1, 0, 0, 0, 0),
    'end_date': datetime(2016, 12, 31, 0, 0, 0, 0),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('immigration_dag', concurrency=1, schedule_interval='@yearly', default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)

# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='immigration_cluster', num_core_nodes=2)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)

# Converts each of the movielens datafile to parquet
def spark_submit_to_emr(**kwargs):
    # ti is the Task Instance
    script_file = kwargs['params']['file']
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    execution_date = kwargs["execution_date"]
    year = execution_date.strftime("%Y")
    logging.info('processing for year '+ str(year))

    statement_response = emr.submit_statement(session_url,
                                              script_file,"year = '" + str(year) + "'\n" )
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


# Define the individual tasks using Python Operators
# Task to Create Cluster
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

# Task to wait until cluster is created
wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

# Task to submit job for immigration pyspark script
transform_immigration = PythonOperator(
    task_id='transform_immigration',
    python_callable=spark_submit_to_emr,
    params={ 'file' : '/root/airflow/dags/transform/immigration.py'},
    dag=dag)

# Task to submit job for city demographics pyspark script
transform_city_demographics = PythonOperator(
    task_id='transform_city_demographics',
    python_callable=spark_submit_to_emr,
    params={ 'file' : '/root/airflow/dags/transform/cities_demographics.py'},
    dag=dag)

# Task to submit job for country codes pyspark script
transform_country_codes = PythonOperator(
    task_id='transform_country_codes',
    python_callable=spark_submit_to_emr,
    params={ 'file' : '/root/airflow/dags/transform/country_codes.py'},
    dag=dag)

# Task to submit job for us visas pyspark script
transform_us_visas = PythonOperator(
    task_id='transform_us_visas',
    python_callable=spark_submit_to_emr,
    params={ 'file' : '/root/airflow/dags/transform/us_visas.py'},
    dag=dag)

# Task to submit job for to check the quality of the tables created
quality_checks = PythonOperator(
    task_id='quality_checks',
    python_callable=spark_submit_to_emr,
    params={ 'file' : '/root/airflow/dags/transform/quality_checks.py'},
    dag=dag)

# Task to submit job for to collect the stats
collect_stats = PythonOperator(
    task_id='collect_stats',
    python_callable=spark_submit_to_emr,
    params={ 'file' : '/root/airflow/dags/transform/collect_stats.py'},
    dag=dag)

# Task to submit job for terminating the EMR cluster
terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# construct the DAG by setting the dependencies
create_cluster >> wait_for_cluster_completion

wait_for_cluster_completion >> transform_immigration 
wait_for_cluster_completion >> transform_city_demographics
wait_for_cluster_completion >> transform_country_codes
wait_for_cluster_completion >> transform_us_visas

transform_immigration >> quality_checks
transform_city_demographics >> quality_checks
transform_country_codes >> quality_checks
transform_us_visas >> quality_checks

quality_checks >> collect_stats 
collect_stats >> terminate_cluster
