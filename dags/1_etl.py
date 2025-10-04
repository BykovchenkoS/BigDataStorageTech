from datetime import datetime, timedelta
import requests
import logging
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import redis
import oracledb as Oracle


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_cheap_tickets(**context):
    logging.info("Starting extraction from Aviasales API")
    API_TOKEN = "b7721379bd50af994b608f3216348cb4"
    destinations = ['LED', 'AER', 'OVB', 'KRR', 'GOJ', 'SVX', 'IST', 'TBS']
    all_tickets = []
    url = "https://api.travelpayouts.com/aviasales/v3/grouped_prices"

    for destination in destinations:
        try:
            params = {
                'origin': 'MOW',
                'destination': destination,
                'currency': 'rub',
                'departure_at': '2025-10',
                'group_by': 'departure_at',
                'token': API_TOKEN
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get('success') and 'data' in data:
                for date_str, ticket_data in data['data'].items():
                    ticket = {
                        'ticket_id': f"{ticket_data.get('airline', '')}_{ticket_data.get('flight_number', '')}_{destination}",
                        'origin': ticket_data.get('origin', 'MOW'),
                        'destination': destination,
                        'departure_at': ticket_data.get('departure_at', ''),
                        'price': ticket_data.get('price', 0),
                        'currency': data.get('currency', 'rub'),
                        'airline': ticket_data.get('airline', ''),
                        'flight_number': ticket_data.get('flight_number', ''),
                        'transfers': ticket_data.get('transfers', 0),
                        'duration': ticket_data.get('duration', 0),
                        'origin_airport': ticket_data.get('origin_airport', ''),
                        'destination_airport': ticket_data.get('destination_airport', ''),
                        'link': ticket_data.get('link', ''),
                        'extracted_at': datetime.now().isoformat(),
                        'route': f"MOW-{destination}",
                        'departure_date': date_str,
                        'data_source': 'aviasales_api_v3'
                    }
                    all_tickets.append(ticket)

                logging.info(f"Found {len(data['data'])} dates with tickets for {destination}")
            else:
                logging.warning(f"No ticket data for {destination}")

            time.sleep(1)

        except Exception as e:
            logging.error(f"ERROR for {destination}: {str(e)}")
            continue

    context['ti'].xcom_push(key='extracted_tickets', value=all_tickets)
    return all_tickets


def transform_tickets(**context):
    logging.info("Starting transformation")

    ti = context['ti']
    raw_tickets = ti.xcom_pull(task_ids='extract_cheap_tickets', key='extracted_tickets')

    if not raw_tickets:
        logging.warning("No tickets to transform")
        return []

    transformed_tickets = []
    for ticket in raw_tickets:
        try:
            transformed_ticket = {
                'ticket_id': ticket['ticket_id'],
                'origin': ticket.get('origin', ''),
                'destination': ticket.get('destination', ''),
                'departure_at': ticket.get('departure_at', ''),
                'price': float(ticket.get('price', 0)),
                'airline': ticket.get('airline', ''),
                'flight_number': ticket.get('flight_number', ''),
                'transfers': int(ticket.get('transfers', 0)),
                'duration': int(ticket.get('duration', 0)),
                'is_direct': ticket.get('transfers', 0) == 0,
                'price_category': 'cheap' if float(ticket.get('price', 0)) < 10000 else 'normal',
                'extracted_at': ticket.get('extracted_at', ''),
                'processed_at': datetime.now().isoformat(),
                'route': ticket.get('route', '')
            }
            transformed_tickets.append(transformed_ticket)
        except Exception as e:
            logging.error(f"ERROR transforming {ticket.get('ticket_id', 'unknown')}: {str(e)}")

    context['ti'].xcom_push(key='transformed_tickets', value=transformed_tickets)
    logging.info(f"Transformed {len(transformed_tickets)} tickets")
    return transformed_tickets


def load_to_mongodb(**context):
    logging.info("Loading to MongoDB")

    ti = context['ti']
    transformed_tickets = ti.xcom_pull(task_ids='transform_tickets', key='transformed_tickets')
    if not transformed_tickets:
        logging.error("Nothing to load into MongoDB")
        return

    try:
        client = MongoClient(host='mongodb-ods', port=27017, username='root', password='example', authSource='admin')
        db = client.aviasales_ods
        collection = db.cheap_tickets
        result = collection.insert_many(transformed_tickets)
        logging.info(f"Loaded {len(result.inserted_ids)} tickets into MongoDB")
        client.close()
    except Exception as e:
        logging.error(f"MongoDB ERROR: {str(e)}")
        raise


def load_to_redis(**context):
    logging.info("Loading to Redis")

    ti = context['ti']
    transformed_tickets = ti.xcom_pull(task_ids='transform_tickets', key='transformed_tickets')
    if not transformed_tickets:
        logging.error("Nothing to load into Redis")
        return

    try:
        r = redis.Redis(host='redis-ods', port=6379, db=0)

        for ticket in transformed_tickets:
            redis_ticket = {}
            for key, value in ticket.items():
                if isinstance(value, bool):
                    redis_ticket[key] = int(value)
                elif isinstance(value, (int, float)):
                    redis_ticket[key] = str(value)
                else:
                    redis_ticket[key] = value

            key = f"ticket:{ticket['ticket_id']}"
            r.hset(key, mapping=redis_ticket)

        logging.info(f"Loaded {len(transformed_tickets)} tickets into Redis")

    except Exception as e:
        logging.error(f"Redis ERROR: {str(e)}")
        raise


def load_to_oracle(**context):
    logging.info("Loading to Oracle")

    ti = context['ti']
    transformed_tickets = ti.xcom_pull(task_ids='transform_tickets', key='transformed_tickets')
    if not transformed_tickets:
        logging.error("Nothing to load into Oracle")
        return

    try:
        dsn = Oracle.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = Oracle.connect(user="system", password="oracle", dsn=dsn)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                CREATE TABLE cheap_tickets (
                    ticket_id VARCHAR2(100),
                    origin VARCHAR2(10),
                    destination VARCHAR2(10),
                    departure_at VARCHAR2(50),
                    price NUMBER,
                    airline VARCHAR2(50),
                    flight_number VARCHAR2(50),
                    transfers NUMBER,
                    duration NUMBER,
                    is_direct NUMBER,
                    price_category VARCHAR2(20),
                    extracted_at VARCHAR2(50),
                    processed_at VARCHAR2(50),
                    route VARCHAR2(20)
                )
            """)
        except Exception:
            logging.info("Table cheap_tickets already exists")

        for ticket in transformed_tickets:
            cursor.execute("""
                INSERT INTO cheap_tickets (ticket_id, origin, destination, departure_at, price, airline, flight_number, 
                transfers, duration, is_direct, price_category, extracted_at, processed_at, route)
                VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)
            """, (
                ticket['ticket_id'], ticket['origin'], ticket['destination'], ticket['departure_at'],
                ticket['price'], ticket['airline'], ticket['flight_number'], ticket['transfers'],
                ticket['duration'], int(ticket['is_direct']), ticket['price_category'],
                ticket['extracted_at'], ticket['processed_at'], ticket['route']
            ))

        conn.commit()
        logging.info(f"Loaded {len(transformed_tickets)} tickets into Oracle")
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Oracle ERROR: {str(e)}")
        raise


with DAG(
        'aviasales_cheap_tickets',
        default_args=default_args,
        description='ETL process for cheap tickets (Mongo, Redis, Oracle)',
        schedule_interval=timedelta(hours=6),
        catchup=False,
        tags=['aviasales', 'cheap_tickets', 'etl'],
) as dag:
    extract_task = PythonOperator(task_id='extract_cheap_tickets', python_callable=extract_cheap_tickets)
    transform_task = PythonOperator(task_id='transform_tickets', python_callable=transform_tickets)
    load_mongo_task = PythonOperator(task_id='load_to_mongodb', python_callable=load_to_mongodb)
    load_redis_task = PythonOperator(task_id='load_to_redis', python_callable=load_to_redis)
    load_oracle_task = PythonOperator(task_id='load_to_oracle', python_callable=load_to_oracle)

    extract_task >> transform_task
    transform_task >> [load_mongo_task, load_redis_task, load_oracle_task]
