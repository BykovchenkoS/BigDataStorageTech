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


def create_dim_airline_scd2_table(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        cursor.execute("""SELECT COUNT(*) FROM user_tables WHERE table_name = 'DIM_AIRLINE'""")
        table_exists = cursor.fetchone()[0] > 0

        if table_exists:
            cursor.execute("""SELECT column_name FROM user_tab_columns WHERE table_name = 'DIM_AIRLINE'""")
            columns = {row[0].upper() for row in cursor.fetchall()}
            required_cols = {'AIRLINE_SK', 'AIRLINE_BK', 'VALID_FROM', 'VALID_TO', 'IS_CURRENT'}

            if not required_cols.issubset(columns):
                logging.info("Existing dim_airline table has incompatible structure — dropping and recreating")
                try:
                    cursor.execute("DROP TABLE fact_flights CASCADE CONSTRAINTS")
                except:
                    pass
                cursor.execute("DROP TABLE dim_airline CASCADE CONSTRAINTS")
            else:
                logging.info("dim_airline table already exists with correct SCD Type 2 structure")
                return

        # создаем таблицу с SCD2 структурой
        cursor.execute("""CREATE TABLE dim_airline (airline_sk NUMBER PRIMARY KEY, airline_bk VARCHAR2(10) NOT NULL,
                            airline_name VARCHAR2(100), country VARCHAR2(50), valid_from DATE NOT NULL, 
                            valid_to DATE, is_current NUMBER(1) DEFAULT 1 NOT NULL)""")

        cursor.execute("""CREATE INDEX idx_airline_current ON dim_airline(airline_bk, is_current)""")
        conn.commit()
        logging.info("dim_airline table with SCD Type 2 structure created SUCCESSFULLY")

    except oracledb.DatabaseError as e:
        logging.error(f"Database error during table creation: {e}")
        raise
    finally:
        if conn:
            conn.close()


def initial_load_dim_airline(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim_airline")
        existing_count = cursor.fetchone()[0]

        if existing_count > 0:
            logging.info(f"dim_airline already contains {existing_count} records, skipping initial load")
            return

        cursor.execute("""SELECT DISTINCT airline FROM cheap_tickets WHERE airline IS NOT NULL""")
        source_airlines = [row[0] for row in cursor.fetchall()]
        logging.info(f"Found {len(source_airlines)} distinct airlines in ODS for initial load")

        current_sk = 1
        loaded_count = 0

        for airline_bk in source_airlines:
            airline_name, country = _get_airline_info(airline_bk)

            cursor.execute("""INSERT INTO dim_airline (airline_sk, airline_bk, airline_name, country, valid_from, 
                                valid_to, is_current) VALUES (:1, :2, :3, :4, :5, NULL, 1)""", (
                current_sk, airline_bk, airline_name, country,
                datetime.now()
            ))

            current_sk += 1
            loaded_count += 1
            logging.info(f"Initial load: added airline {airline_bk} ({airline_name})")

        conn.commit()
        logging.info(f"Initial load completed: {loaded_count} airlines loaded into dim_airline")

    except Exception as e:
        logging.error(f"Error during initial load of dim_airline: {e}")
        raise
    finally:
        if conn:
            conn.close()


def update_dim_airline_scd2(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        cursor.execute("""SELECT DISTINCT airline AS airline_bk FROM cheap_tickets WHERE airline IS NOT NULL""")
        source_airlines = [row[0] for row in cursor.fetchall()]
        logging.info(f"Found {len(source_airlines)} distinct airlines in ODS for SCD2 update")

        cursor.execute("SELECT NVL(MAX(airline_sk), 0) FROM dim_airline")
        max_sk = cursor.fetchone()[0]

        updated_count = 0
        new_count = 0
        unchanged_count = 0

        for airline_bk in source_airlines:
            current_name, current_country = _get_airline_info(airline_bk)

            # ищем текущую версию авиакомпании
            cursor.execute("""SELECT airline_sk, airline_name, country FROM dim_airline 
                                WHERE airline_bk = :bk AND is_current = 1""", (airline_bk,))
            current_version = cursor.fetchone()

            if current_version:
                current_sk, existing_name, existing_country = current_version

                # проверяем, изменились ли данные
                if existing_name != current_name or existing_country != current_country:
                    # закрываем текущую версию
                    cursor.execute("""UPDATE dim_airline SET valid_to = :now, is_current = 0 WHERE airline_sk = :sk""",
                                   (datetime.now(), current_sk))

                    # создаем новую версию
                    max_sk += 1
                    cursor.execute("""INSERT INTO dim_airline (airline_sk, airline_bk, airline_name, country, 
                                        valid_from, valid_to, is_current) VALUES (:1, :2, :3, :4, :5, NULL, 1)""",
                                   (max_sk, airline_bk, current_name, current_country,datetime.now())
                                   )

                    updated_count += 1
                    logging.info(f"SCD2 update: {airline_bk} - created new version {max_sk}")
                else:
                    unchanged_count += 1
                    logging.debug(f"SCD2: {airline_bk} - no changes detected")
            else:
                # новая авиакомпания
                max_sk += 1
                cursor.execute("""INSERT INTO dim_airline (airline_sk, airline_bk, airline_name, country, valid_from, 
                                    valid_to, is_current) VALUES (:1, :2, :3, :4, :5, NULL, 1)""",
                               (max_sk, airline_bk, current_name, current_country,datetime.now())
                               )
                new_count += 1
                logging.info(f"SCD2: {airline_bk} - new airline added as version {max_sk}")

        conn.commit()
        logging.info(f"SCD2 update completed: {updated_count} updated, {new_count} new, {unchanged_count} unchanged")

        # статистика после обновления
        cursor.execute("SELECT COUNT(*) FROM dim_airline WHERE is_current = 1")
        current_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dim_airline")
        total_count = cursor.fetchone()[0]

        logging.info(f"Final stats: {current_count} current versions, {total_count} total records")

    except Exception as e:
        logging.error(f"Error updating dim_airline with SCD2: {e}")
        raise
    finally:
        if conn:
            conn.close()


def validate_dim_airline_data(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        # проверяем целостность данных
        checks_passed = True

        # проверяем, что у каждой авиакомпании есть только одна текущая версия
        cursor.execute("""SELECT airline_bk, COUNT(*) as current_count FROM dim_airline WHERE is_current = 1 
                            GROUP BY airline_bk HAVING COUNT(*) > 1""")
        multiple_current = cursor.fetchall()

        if multiple_current:
            logging.error(f"Found airlines with multiple current versions: {multiple_current}")
            checks_passed = False

        # проверяем, что нет авиакомпаний без текущей версии
        cursor.execute("""SELECT DISTINCT airline_bk FROM dim_airline WHERE airline_bk NOT IN 
            (SELECT airline_bk FROM dim_airline WHERE is_current = 1)""")
        no_current = cursor.fetchall()

        if no_current:
            logging.error(f"Found airlines without current version: {no_current}")
            checks_passed = False

        # проверяем ссылочную целостность с ODS
        cursor.execute("""SELECT COUNT(DISTINCT airline) as missing_airlines FROM cheap_tickets 
                            WHERE airline IS NOT NULL AND airline NOT IN (SELECT airline_bk FROM dim_airline 
                            WHERE is_current = 1)""")
        missing_in_dim = cursor.fetchone()[0]

        if missing_in_dim > 0:
            logging.warning(f"Found {missing_in_dim} airlines in ODS missing from dim_airline current versions")

        # статистика по версиям
        cursor.execute("""SELECT COUNT(*) as total_records, COUNT(DISTINCT airline_bk) as unique_airlines, 
                            SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) as current_versions, 
                            SUM(CASE WHEN is_current = 0 THEN 1 ELSE 0 END) as historical_versions FROM dim_airline""")
        stats = cursor.fetchone()
        total_records, unique_airlines, current_versions, historical_versions = stats

        logging.info("dim_airline validation statistics:")
        logging.info(f"  - Total records: {total_records}")
        logging.info(f"  - Unique airlines: {unique_airlines}")
        logging.info(f"  - Current versions: {current_versions}")
        logging.info(f"  - Historical versions: {historical_versions}")

        if checks_passed:
            logging.info("All dim_airline data validation checks passed")
        else:
            logging.error("Some dim_airline data validation checks failed")

        return checks_passed

    except Exception as e:
        logging.error(f"Error during dim_airline validation: {e}")
        return False
    finally:
        if conn:
            conn.close()


def _get_airline_info(airline_code):
    mapping = {
        'SU': ('Aeroflot', 'Russia'),
        'S7': ('S7 Airlines', 'Russia'),
        'U6': ('Ural Airlines', 'Russia'),
        'FV': ('Rossiya Airlines', 'Russia'),
        'TK': ('Turkish Airlines', 'Turkey'),
        'DP': ('Pobeda', 'Russia'),
        'A4': ('Azimuth', 'Russia'),
        'UT': ('UTair', 'Russia'),
        'N4': ('Nordwind', 'Russia'),
        '7R': ('Russkie Krylya', 'Russia'),
        'WZ': ('Red Wings', 'Russia'),
        '5N': ('Smartavia', 'Russia'),
        'D2': ('Severstal Air Company', 'Russia'),
        'Y7': ('Nordstar Airlines', 'Russia'),
        '6W': ('Saratov Airlines', 'Russia'),
        'B2': ('Belavia', 'Belarus'),
        '2S': ('Southwind Airlines', 'Russia'),
        '6R': ('ALROSA', 'Russia'),

    }
    return mapping.get(airline_code, (airline_code, 'Unknown'))


with DAG('dds_airline_scd2', default_args=default_args, description='Creates and maintains dim_airline with SCD2 support',
         schedule=None, catchup=False, tags=['aviasales', 'dds', 'scd2', 'dimensions', 'airline']
         ) as dag:

    create_table_task = PythonOperator(task_id='create_dim_airline_scd2_table', python_callable=create_dim_airline_scd2_table)
    initial_load_task = PythonOperator(task_id='initial_load_dim_airline', python_callable=initial_load_dim_airline)
    update_scd2_task = PythonOperator(task_id='update_dim_airline_scd2', python_callable=update_dim_airline_scd2)
    validate_data_task = PythonOperator(task_id='validate_dim_airline_data', python_callable=validate_dim_airline_data)

    create_table_task >> initial_load_task >> update_scd2_task >> validate_data_task