from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import oracledb
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_data_quality(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        # сравниваем объемы данных
        cursor.execute("SELECT COUNT(*) FROM cheap_tickets")
        ods = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM fact_flights")
        dds = cursor.fetchone()[0]
        success = round(dds / ods * 100, 2) if ods > 0 else 0

        # проверяем наличие аномальных цен
        cursor.execute("SELECT COUNT(*) FROM fact_flights WHERE price <= 0 OR price > 1000000")
        price_err = cursor.fetchone()[0]

        # проверяем целостность связей, все ли внешние ключи заполнены
        cursor.execute("SELECT COUNT(*) FROM fact_flights WHERE route_id IS NULL OR airline_sk IS NULL OR date_id IS NULL")
        nulls = cursor.fetchone()[0]

        logging.info(f"Data volume check: ODS = {ods}, DDS = {dds}, Success rate = {success}%")
        logging.info(f"Price validation: {price_err} records with invalid prices detected")
        logging.info(f"Referential integrity: {nulls} records with NULL foreign keys detected")

        if price_err == 0 and nulls == 0 and success >= 90:
            logging.info("All data quality checks passed successfully")
        else:
            logging.warning("One or more data quality checks failed")

    except Exception as e:
        logging.error(f"Data quality validation failed with error: {e}")
        raise
    finally:
        if conn:
            conn.close()


with DAG('data_quality_checks', default_args=default_args, description='Performs data quality validation on the DDS layer, including volume comparison, anomaly detection, and referential integrity checks',
         schedule=None, catchup=False, tags=['aviasales', 'data_quality', 'dds']
         ) as dag:

    PythonOperator(task_id='check_data_quality', python_callable=check_data_quality)
