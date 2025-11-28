from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import oracledb
import logging
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


def create_fact_flights_table(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE fact_flights (
                flight_id VARCHAR2(100) PRIMARY KEY,
                route_id NUMBER NOT NULL,
                airline_sk NUMBER NOT NULL,  -- Ссылка на SCD2 измерение
                date_id NUMBER NOT NULL,
                flight_type_id NUMBER NOT NULL,
                price NUMBER NOT NULL,
                duration NUMBER NOT NULL,
                extracted_at TIMESTAMP NOT NULL,
                CONSTRAINT fk_fact_route FOREIGN KEY (route_id) REFERENCES dim_route(route_id),
                CONSTRAINT fk_fact_airline FOREIGN KEY (airline_sk) REFERENCES dim_airline(airline_sk),  -- Ссылка на SCD2
                CONSTRAINT fk_fact_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
                CONSTRAINT fk_fact_flight_type FOREIGN KEY (flight_type_id) REFERENCES dim_flight_type(flight_type_id)
            )
        """)

        cursor.execute("""
            CREATE INDEX idx_fact_flights_route ON fact_flights(route_id, date_id)
        """)
        cursor.execute("""
            CREATE INDEX idx_fact_flights_airline ON fact_flights(airline_sk, date_id)
        """)
        cursor.execute("""
            CREATE INDEX idx_fact_flights_date ON fact_flights(date_id)
        """)

        conn.commit()
        logging.info("fact_flights table created successfully with SCD2 support")

    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            logging.info("fact_flights table already exists")
        else:
            logging.error(f"Error creating fact_flights table: {e}")
            raise
    finally:
        if conn:
            conn.close()


def parse_date(date_str):
    if not date_str:
        return None

    try:
        formats = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d.%m.%Y %H:%M:%S',
            '%d.%m.%Y',
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt
            except ValueError:
                continue

        match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
        if match:
            return datetime.strptime(match.group(1), '%Y-%m-%d')

        logging.warning(f"Unable to parse date: {date_str}")
        return None

    except Exception as e:
        logging.warning(f"Error parsing date '{date_str}': {e}")
        return None


def populate_fact_flights(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM fact_flights")
        logging.info("fact_flights table cleared")

        cursor.execute("""SELECT ticket_id, origin, destination, departure_at, price, airline, transfers, duration, extracted_at 
                            FROM cheap_tickets WHERE origin IS NOT NULL AND destination IS NOT NULL AND departure_at IS NOT NULL 
                            AND price IS NOT NULL AND airline IS NOT NULL""")

        rows = cursor.fetchall()
        logging.info(f"Found {len(rows)} records for processing")

        inserted_count = 0
        error_count = 0
        skipped_route = 0
        skipped_airline = 0

        for row in rows:
            try:
                ticket_id, origin, destination, departure_at, price, airline, transfers, duration, extracted_at = row
                departure_dt = parse_date(departure_at)
                extracted_dt = parse_date(extracted_at)

                if not departure_dt:
                    logging.warning(f"Skipped record {ticket_id}: invalid departure_at format: {departure_at}")
                    error_count += 1
                    continue

                date_id = int(departure_dt.strftime('%Y%m%d'))

                if transfers == 0:
                    flight_type_id = 1  # direct
                elif transfers == 1:
                    flight_type_id = 2  # one_stop
                else:
                    flight_type_id = 3  # multi_stop

                cursor.execute("""SELECT r.route_id FROM dim_route r JOIN dim_airport o ON r.origin_airport_id = o.airport_id
                                    JOIN dim_airport d ON r.destination_airport_id = d.airport_id WHERE o.airport_code = :1 AND d.airport_code = :2""",
                               (origin, destination))
                route_result = cursor.fetchone()

                if not route_result:
                    logging.warning(f"Skipped record {ticket_id}: route not found for {origin}-{destination}")
                    skipped_route += 1
                    error_count += 1
                    continue

                cursor.execute("""SELECT airline_sk FROM dim_airline WHERE airline_bk = :1 AND is_current = 1""", (airline,))
                airline_result = cursor.fetchone()

                if not airline_result:
                    logging.warning(f"Skipped record {ticket_id}: current airline version not found for {airline}")
                    skipped_airline += 1
                    error_count += 1
                    continue

                route_id = route_result[0]
                airline_sk = airline_result[0]

                cursor.execute("""INSERT INTO fact_flights (flight_id, route_id, airline_sk, date_id, flight_type_id, 
                                    price, duration, extracted_at) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)""",
                               (ticket_id, route_id, airline_sk, date_id, flight_type_id,float(price), int(duration),
                                extracted_dt or datetime.now())
                               )

                inserted_count += 1

                if inserted_count % 100 == 0:
                    logging.info(f"Progress: {inserted_count} records inserted...")

            except Exception as e:
                logging.error(f"Error processing record {ticket_id}: {e}")
                error_count += 1
                continue

        conn.commit()

        logging.info("=" * 50)
        logging.info("FACT FLIGHTS LOADING COMPLETED")
        logging.info("=" * 50)
        logging.info(f"Successfully inserted: {inserted_count} records")
        logging.info(f"Skipped due to errors: {error_count} records")
        logging.info(f"     Missing routes: {skipped_route}")
        logging.info(f"     Missing airlines: {skipped_airline}")
        logging.info(f"Success rate: {round(inserted_count / len(rows) * 100, 2)}%")

        cursor.execute("SELECT COUNT(*) FROM fact_flights")
        final_count = cursor.fetchone()[0]
        logging.info(f"Final row count in fact_flights: {final_count}")

    except Exception as e:
        logging.error(f"Error populating fact_flights: {e}")
        raise
    finally:
        if conn:
            conn.close()


def validate_fact_flights_data(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        logging.info("Starting fact_flights data validation...")

        # 1 - проверяем общее количество записей
        cursor.execute("SELECT COUNT(*) FROM fact_flights")
        total_count = cursor.fetchone()[0]
        logging.info(f"Total records in fact_flights: {total_count}")

        # 2 - проверяем целостность внешних ключей
        cursor.execute("""SELECT COUNT(*) as broken_links FROM fact_flights f
                            WHERE NOT EXISTS (SELECT 1 FROM dim_route r WHERE r.route_id = f.route_id)
                                OR NOT EXISTS (SELECT 1 FROM dim_airline a WHERE a.airline_sk = f.airline_sk)
                                OR NOT EXISTS (SELECT 1 FROM dim_date d WHERE d.date_id = f.date_id)
                                OR NOT EXISTS (SELECT 1 FROM dim_flight_type ft WHERE ft.flight_type_id = f.flight_type_id)""")
        broken_links = cursor.fetchone()[0]

        if broken_links == 0:
            logging.info("All foreign key constraints are valid")
        else:
            logging.error(f"Found {broken_links} records with broken foreign key links")

        # 3 - проверяем аномальные цены
        cursor.execute("""SELECT COUNT(*) as price_anomalies FROM fact_flights WHERE price <= 0 OR price > 1000000""")
        price_anomalies = cursor.fetchone()[0]

        if price_anomalies == 0:
            logging.info("No price anomalies detected")
        else:
            logging.warning(f"Found {price_anomalies} records with price anomalies")

        # 4 - проверяем статистику по авиакомпаниям
        cursor.execute("""SELECT a.airline_bk, a.airline_name, COUNT(*) as flight_count, ROUND(AVG(f.price), 2) as avg_price 
                            FROM fact_flights f JOIN dim_airline a ON f.airline_sk = a.airline_sk WHERE a.is_current = 1
                                GROUP BY a.airline_bk, a.airline_name ORDER BY flight_count DESC""")
        airline_stats = cursor.fetchall()

        logging.info("Top airlines by flight count:")
        for airline_bk, airline_name, flight_count, avg_price in airline_stats[:5]:
            logging.info(f"     {airline_bk} ({airline_name}): {flight_count} flights, avg price: {avg_price}")

        # 5 - проверяем статистику по типам перелетов
        cursor.execute("""SELECT ft.type_category, COUNT(*) as flight_count, ROUND(AVG(f.price), 2) as avg_price
                            FROM fact_flights f JOIN dim_flight_type ft ON f.flight_type_id = ft.flight_type_id
                                GROUP BY ft.type_category ORDER BY flight_count DESC""")
        flight_type_stats = cursor.fetchall()

        logging.info("Flight type statistics:")
        for type_category, flight_count, avg_price in flight_type_stats:
            logging.info(f"  - {type_category}: {flight_count} flights, avg price: {avg_price}")

        # 6 - проверяем покрытие дат
        cursor.execute("""SELECT MIN(d.full_date) as earliest_date, MAX(d.full_date) as latest_date, 
                            COUNT(DISTINCT f.date_id) as unique_dates FROM fact_flights f JOIN dim_date d ON f.date_id = d.date_id""")
        date_coverage = cursor.fetchone()
        earliest_date, latest_date, unique_dates = date_coverage

        logging.info(f"Date coverage: {earliest_date} to {latest_date} ({unique_dates} unique dates)")

        if broken_links == 0 and price_anomalies == 0 and total_count > 0:
            logging.info("All fact_flights data validation checks passed")
            return True
        else:
            logging.warning("Some fact_flights data validation checks have warnings")
            return False

    except Exception as e:
        logging.error(f"Error during fact_flights validation: {e}")
        return False
    finally:
        if conn:
            conn.close()


def debug_data_sources(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        logging.info("Data sources debugging information:")

        # проверяем ODS данные
        cursor.execute("SELECT COUNT(*) FROM cheap_tickets")
        ods_count = cursor.fetchone()[0]
        logging.info(f"ODS records (cheap_tickets): {ods_count}")

        # проверяем измерения
        cursor.execute("SELECT COUNT(*) FROM dim_route")
        route_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dim_airline WHERE is_current = 1")
        airline_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dim_flight_type")
        flight_type_count = cursor.fetchone()[0]

        logging.info(f"Dimension records:")
        logging.info(f"     dim_route: {route_count}")
        logging.info(f"     dim_airline (current): {airline_count}")
        logging.info(f"     dim_flight_type: {flight_type_count}")

        cursor.execute("""SELECT DISTINCT origin, destination, airline FROM cheap_tickets WHERE ROWNUM <= 5""")
        sample_routes = cursor.fetchall()
        logging.info("Sample ODS routes:")
        for origin, destination, airline in sample_routes:
            logging.info(f" {origin} -> {destination} ({airline})")

    except Exception as e:
        logging.error(f"Error during data sources debugging: {e}")
    finally:
        if conn:
            conn.close()


with DAG('dds_fact_flights', default_args=default_args, description='Creates and populates the fact table for flight records with SCD2 support',
         schedule=None, catchup=False, tags=['aviasales', 'dds', 'fact', 'scd2']
         ) as dag:

    debug_task = PythonOperator(task_id='debug_data_sources', python_callable=debug_data_sources)
    create_table_task = PythonOperator(task_id='create_fact_flights_table', python_callable=create_fact_flights_table)
    populate_data_task = PythonOperator(task_id='populate_fact_flights', python_callable=populate_fact_flights)
    validate_data_task = PythonOperator(task_id='validate_fact_flights_data', python_callable=validate_fact_flights_data)
    trigger_dq_task = TriggerDagRunOperator(task_id='trigger_data_quality', trigger_dag_id='data_quality_checks', wait_for_completion=False)

    debug_task >> create_table_task >> populate_data_task >> validate_data_task >> trigger_dq_task