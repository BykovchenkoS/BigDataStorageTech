import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.models import DagBag
from airflow.operators.python import PythonOperator


def get_execution_date(logical_date, **kwargs):
    return logical_date


def debug_dag_status(**context):
    dag_bag = DagBag()
    target_dags = [
        'dds_airline_scd2',
        'dds_dimensions',
        'dds_fact_flights',
        'data_quality_checks'
    ]
    for dag_id in target_dags:
        if dag_id in dag_bag.dags:
            logging.info(f"DAG {dag_id} found in DagBag")
        else:
            logging.error(f"DAG {dag_id} NOT found in DagBag")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='dds_coordinator_sensors', default_args=default_args,
         description='DDS pipeline coordinator with proper dependencies using sensors',
         schedule=None, catchup=False, tags=['aviasales', 'dds', 'coordinator', 'sensors']
         ) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')

    debug_task = PythonOperator(task_id='debug_dag_status', python_callable=debug_dag_status)

    trigger_airline = TriggerDagRunOperator(task_id='trigger_airline_scd2', trigger_dag_id='dds_airline_scd2',
                                            wait_for_completion=False, reset_dag_run=True,
                                            execution_date='{{ logical_date }}')

    trigger_dimensions = TriggerDagRunOperator(task_id='trigger_other_dimensions', trigger_dag_id='dds_dimensions',
                                               wait_for_completion=False, reset_dag_run=True,
                                               execution_date='{{ logical_date }}')

    wait_airline_complete = ExternalTaskSensor(task_id='wait_airline_scd2_complete', external_dag_id='dds_airline_scd2',
                                               external_task_id='validate_dim_airline_data',
                                               execution_date_fn=get_execution_date,
                                               timeout=3600, poke_interval=30, mode='reschedule')

    wait_dimensions_complete = ExternalTaskSensor(task_id='wait_dimensions_complete', external_dag_id='dds_dimensions',
                                                  external_task_id='check_dimension_data',
                                                  execution_date_fn=get_execution_date,
                                                  timeout=3600, poke_interval=30, mode='reschedule')
    trigger_facts = TriggerDagRunOperator(task_id='trigger_fact_flights', trigger_dag_id='dds_fact_flights',
                                          wait_for_completion=False, reset_dag_run=True,
                                          execution_date='{{ logical_date }}')

    wait_facts_complete = ExternalTaskSensor(task_id='wait_facts_complete', external_dag_id='dds_fact_flights',
                                             external_task_id='validate_fact_flights_data',
                                             execution_date_fn=get_execution_date,
                                             timeout=3600, poke_interval=30, mode='reschedule')

    trigger_quality = TriggerDagRunOperator(task_id='trigger_quality_checks', trigger_dag_id='data_quality_checks',
                                            wait_for_completion=True, reset_dag_run=True,
                                            execution_date='{{ logical_date }}')

    pipeline_complete = DummyOperator(task_id='pipeline_complete')

    start_pipeline >> debug_task >> [trigger_airline, trigger_dimensions]
    trigger_airline >> wait_airline_complete
    trigger_dimensions >> wait_dimensions_complete
    [wait_airline_complete, wait_dimensions_complete] >> trigger_facts
    trigger_facts >> wait_facts_complete >> trigger_quality >> pipeline_complete