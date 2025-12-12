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


def create_dim_date(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE dim_date (date_id NUMBER PRIMARY KEY, full_date DATE NOT NULL, year NUMBER(4) NOT NULL, 
                            quarter NUMBER(1) NOT NULL, month NUMBER(2) NOT NULL, month_name VARCHAR2(20) NOT NULL, 
                            day NUMBER(2) NOT NULL, day_of_week NUMBER(1) NOT NULL, day_name VARCHAR2(10) NOT NULL,
                            is_weekend NUMBER(1) NOT NULL)""")
        from datetime import datetime as dt
        start = dt(2020, 1, 1)
        end = dt(2025, 12, 31)
        current = start
        month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        while current <= end:
            date_id = int(current.strftime('%Y%m%d'))
            is_weekend = 1 if current.weekday() >= 5 else 0
            cursor.execute("""INSERT INTO dim_date VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)""",
                           (date_id, current, current.year, (current.month - 1) // 3 + 1, current.month,
                            month_names[current.month - 1], current.day, current.weekday() + 1, day_names[current.weekday()], is_weekend))
            current += timedelta(days=1)

        conn.commit()
        logging.info("dim_date table created successfully")

    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            logging.info("dim_date already exists")
        else:
            raise
    finally:
        if conn:
            conn.close()


def create_dim_airline(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE dim_airline (airline_id NUMBER PRIMARY KEY, airline_code VARCHAR2(10) NOT NULL UNIQUE,
                            airline_name VARCHAR2(100), country VARCHAR2(50))""")
        data = [
            (1, 'SU', 'Aeroflot', 'Russia'),
            (2, 'S7', 'S7 Airlines', 'Russia'),
            (3, 'U6', 'Ural Airlines', 'Russia'),
            (4, 'FV', 'Rossiya Airlines', 'Russia'),
            (5, 'TK', 'Turkish Airlines', 'Turkey'),
            (6, 'DP', 'Pobeda', 'Russia'),
            (7, 'A4', 'Azimuth', 'Russia'),
            (8, 'UT', 'UTair', 'Russia'),
            (9, 'N4', 'Nordwind', 'Russia'),
            (10, '7R', 'Russkie Krylya', 'Russia')
        ]
        cursor.executemany("INSERT INTO dim_airline VALUES (:1, :2, :3, :4)", data)
        conn.commit()
        logging.info("dim_airline created")

    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            logging.info("dim_airline already exists")
        else:
            raise
    finally:
        if conn:
            conn.close()


def create_dim_airport(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        cursor.execute("""CREATE TABLE dim_airport (airport_id NUMBER PRIMARY KEY, airport_code VARCHAR2(10) NOT NULL UNIQUE,
                            city VARCHAR2(50) NOT NULL, country VARCHAR2(50) NOT NULL)""")

        airports_data = [
            (1, 'MOW', 'Moscow', 'Russia'),
            (2, 'LED', 'Saint Petersburg', 'Russia'),
            (3, 'AER', 'Sochi', 'Russia'),
            (4, 'OVB', 'Novosibirsk', 'Russia'),
            (5, 'KRR', 'Krasnodar', 'Russia'),
            (6, 'GOJ', 'Nizhny Novgorod', 'Russia'),
            (7, 'SVX', 'Yekaterinburg', 'Russia'),
            (8, 'IST', 'Istanbul', 'Turkey'),
            (9, 'TBS', 'Tbilisi', 'Georgia'),
            (10, 'VKO', 'Moscow', 'Russia'),
            (11, 'DME', 'Moscow', 'Russia')
        ]
        cursor.executemany("INSERT INTO dim_airport VALUES (:1, :2, :3, :4)", airports_data)
        conn.commit()
        logging.info("dim_airport created successfully")

    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            logging.info("dim_airport already exists")
        else:
            raise
    finally:
        if conn:
            conn.close()


def create_dim_route_normalized(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        cursor.execute("""CREATE TABLE dim_route (route_id NUMBER PRIMARY KEY, origin_airport_id NUMBER NOT NULL,
                            destination_airport_id NUMBER NOT NULL, distance_km NUMBER, popularity_category VARCHAR2(20),
                            CONSTRAINT fk_route_origin FOREIGN KEY (origin_airport_id) REFERENCES dim_airport(airport_id),
                            CONSTRAINT fk_route_destination FOREIGN KEY (destination_airport_id) REFERENCES dim_airport(airport_id))""")

        cursor.execute("""INSERT INTO dim_route (route_id, origin_airport_id, destination_airport_id)
                            SELECT ROWNUM, o.airport_id, d.airport_id FROM (
                            SELECT DISTINCT ct.origin, ct.destination FROM cheap_tickets ct 
                                WHERE ct.origin IS NOT NULL AND ct.destination IS NOT NULL) routes
                            JOIN dim_airport o ON routes.origin = o.airport_code
                            JOIN dim_airport d ON routes.destination = d.airport_code
                            WHERE o.airport_id IS NOT NULL AND d.airport_id IS NOT NULL""")
        conn.commit()
        logging.info("dim_route normalized created successfully")

    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            logging.info("dim_route already exists")
        else:
            raise
    finally:
        if conn:
            conn.close()


def create_dim_flight_type(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        cursor.execute("""CREATE TABLE dim_flight_type (flight_type_id NUMBER PRIMARY KEY, transfers_count NUMBER NOT NULL, 
                            type_category VARCHAR2(20) NOT NULL, description VARCHAR2(100))""")

        data = [
            (1, 0, 'direct', 'Прямой рейс без пересадок'),
            (2, 1, 'one_stop', 'Рейс с одной пересадкой'),
            (3, 2, 'multi_stop', 'Рейс с двумя и более пересадками')
        ]
        cursor.executemany("INSERT INTO dim_flight_type VALUES (:1, :2, :3, :4)", data)

        conn.commit()
        logging.info("dim_flight_type created successfully")

    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            logging.info("dim_flight_type already exists")
        else:
            raise
    finally:
        if conn:
            conn.close()


def check_dimension_data(**context):
    conn = None
    try:
        dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = oracledb.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM dim_route")
        route_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dim_airline")
        airline_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dim_date")
        date_count = cursor.fetchone()[0]

        logging.info("Dimension table row counts:")
        logging.info(f"-- dim_route: {route_count} records")
        logging.info(f"-- dim_airline: {airline_count} records")
        logging.info(f"-- dim_date: {date_count} records")

        cursor.execute("""SELECT DISTINCT airline FROM cheap_tickets WHERE airline IS NOT NULL AND airline NOT IN (SELECT airline_code FROM dim_airline)""")
        missing_airlines = cursor.fetchall()

        if missing_airlines:
            logging.warning(f"Missing airlines in dim_airline: {[a[0] for a in missing_airlines]}")

    except Exception as e:
        logging.error(f"Error during dimension data validation: {e}")
    finally:
        if conn:
            conn.close()


with DAG('dds_dimensions', default_args=default_args, description='Creates and validates DDS dimension tables',
         schedule=None, catchup=False, tags=['aviasales', 'dds', 'dimensions']
         ) as dag:

    t1 = PythonOperator(task_id='create_dim_date', python_callable=create_dim_date)
    t2 = PythonOperator(task_id='create_dim_airline', python_callable=create_dim_airline)
    create_airport_task = PythonOperator(task_id='create_dim_airport', python_callable=create_dim_airport)
    create_route_task = PythonOperator(task_id='create_dim_route_normalized', python_callable=create_dim_route_normalized)
    create_flight_type_task = PythonOperator(task_id='create_dim_flight_type', python_callable=create_dim_flight_type)
    check_dims_task = PythonOperator(task_id='check_dimension_data', python_callable=check_dimension_data)

    [t1, t2, create_airport_task] >> create_route_task >> create_flight_type_task >> check_dims_task