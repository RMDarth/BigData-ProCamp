"""BigData ProCamp lab8 DAG checks the sensor (file on GCS), 
creates a Cloud Dataproc cluster, runs 2 Spark jobs in parallel
and deletes the cluster.

This DAG relies on three Airflow variables
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataproc cluster should be
  created, e.g. "us-central1-c"
* gcs_bucket - Google Cloud Storage bucket to use for input and output of 
Spark job. Input CSV files should be in flights/input folder, jars should be 
in jar subfolder (with names sparkdf1_2.12-1.0.jar and sparkdf2_2.12-1.0.jar)
"""

import datetime
import pendulum
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStorageObjectSensor
from airflow.utils import trigger_rule

current_time = datetime.datetime.now()
folder_name = current_time.strftime('%Y%m%d-%H%M%S')
bucket_name = models.Variable.get('gcs_bucket')

# Output files
task1_output_file = os.path.join(
    bucket_name, 'flights', 'pop_airports',
    folder_name) + os.sep

task2_output_file = os.path.join(
    models.Variable.get('gcs_bucket'), 'flights', 'canceled',
    folder_name) + os.sep

# Path to jar files
TASK1_JAR = (
    os.path.join(bucket_name, 'jars/sparkdf1_2.12-1.0.jar')
)
TASK2_JAR = (
    os.path.join(bucket_name, 'jars/sparkdf2_2.12-1.0.jar')
)
# Arguments to pass to Cloud Dataproc job.
flights_file = os.path.join(bucket_name, 'flights/input/flights.csv')
airports_file = os.path.join(bucket_name, 'flights/input/airports.csv')
airlines_file = os.path.join(bucket_name, 'flights/input/airlines.csv')
task1_args = [flights_file, airports_file, task1_output_file]
task2_args = [flights_file, airports_file, airlines_file, task2_output_file]

default_dag_args = {
    'start_date': datetime.datetime(2020, 1, 1),

    'email_on_failure': False,
    'email_on_retry': False,

    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=3),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'bdpc_lab8_orchestration',
        schedule_interval='@hourly',
        default_args=default_dag_args,
        catchup=False) as dag:

    # Sensor to check file in GCS -> gc://${bucket_name}/flights/${yyyy}/${MM}/${dd}/${HH}/_SUCCESS .
    gcs_file_sensor = GoogleCloudStorageObjectSensor(
        task_id='check_gcs_file_sensor',
        timeout=120,
        bucket='barinov_bdpc',
        soft_fail=True, # This will skip the DAG if file not found, but alternatively this could be set to False to fail the DAG.
        object='flights/{{ execution_date.format("%Y/%m/%d/%H") }}/_SUCCESS')

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='lab8-work-cluster-{{ ds_nodash }}',
        num_workers=2,
        zone=models.Variable.get('gce_zone'),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    # Run Spark job - Popular airports
    run_dataproc_spark_t1 = dataproc_operator.DataProcSparkOperator(
        task_id='run_dataproc_spark_task1',
        dataproc_spark_jars=[TASK1_JAR],
        job_name='popular_airports',
        main_class = 'com.github.rmdarth.bdpclab6.PopularAirportsDF',
        cluster_name='lab8-work-cluster-{{ ds_nodash }}',
        arguments=task1_args
    )

    # Run Spark job - Canceled flights
    run_dataproc_spark_t2 = dataproc_operator.DataProcSparkOperator(
        task_id='run_dataproc_spark_task2',
        dataproc_spark_jars=[TASK2_JAR],
        job_name='canceled_flights',
        main_class = 'com.github.rmdarth.bdpclab6.CanceledFlightsDF',
        cluster_name='lab8-work-cluster-{{ ds_nodash }}',
        arguments=task2_args
    )

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='lab8-work-cluster-{{ ds_nodash }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    gcs_file_sensor >> create_dataproc_cluster

    create_dataproc_cluster >> run_dataproc_spark_t1
    create_dataproc_cluster >> run_dataproc_spark_t2

    run_dataproc_spark_t1 >> delete_dataproc_cluster
    run_dataproc_spark_t2 >> delete_dataproc_cluster
