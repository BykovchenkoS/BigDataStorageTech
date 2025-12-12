from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('dds_coordinator_triggers', default_args=default_args, description='Coordinates DDS pipeline execution using triggers only',
         schedule_interval=None, catchup=False, tags=['aviasales', 'dds', 'coordinator']
         ) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')

    trigger_cheap_tickets = TriggerDagRunOperator(task_id='trigger_cheap_tickets', trigger_dag_id='aviasales_cheap_tickets',
                                                  wait_for_completion=True, reset_dag_run=False, execution_date='{{ ds }}')

    # trigger_popular_directions = TriggerDagRunOperator(task_id='trigger_popular_directions', trigger_dag_id='aviasales_popular_directions',
    #                                                    wait_for_completion=True, reset_dag_run=False, execution_date='{{ ds }}')

    # trigger_price_trends = TriggerDagRunOperator(task_id='trigger_price_trends', trigger_dag_id='aviasales_price_trends',
    #                                              wait_for_completion=True, reset_dag_run=False, execution_date='{{ ds }}')

    trigger_airline_scd2 = TriggerDagRunOperator(task_id='trigger_airline_scd2', trigger_dag_id='dds_airline_scd2',
                                                 wait_for_completion=True, reset_dag_run=True)

    trigger_dimensions = TriggerDagRunOperator(task_id='trigger_dds_dimensions', trigger_dag_id='dds_dimensions',
                                               wait_for_completion=True, reset_dag_run=True)

    trigger_fact = TriggerDagRunOperator(task_id='trigger_dds_fact', trigger_dag_id='dds_fact_flights',
                                         wait_for_completion=True, reset_dag_run=True)

    trigger_data_quality = TriggerDagRunOperator(task_id='trigger_data_quality', trigger_dag_id='data_quality_checks',
                                                 wait_for_completion=True, reset_dag_run=True)

    pipeline_complete = DummyOperator(task_id='pipeline_complete')

    # start_pipeline >> [trigger_cheap_tickets, trigger_popular_directions, trigger_price_trends]
    # [trigger_cheap_tickets, trigger_popular_directions, trigger_price_trends] >> trigger_airline_scd2
    # trigger_airline_scd2 >> trigger_dimensions >> trigger_fact >> trigger_data_quality >> pipeline_complete

    start_pipeline >> trigger_cheap_tickets
    trigger_cheap_tickets >> [trigger_airline_scd2, trigger_dimensions]
    [trigger_airline_scd2, trigger_dimensions] >> trigger_fact
    trigger_fact >> trigger_data_quality >> pipeline_complete
