from datetime import datetime, timedelta
import requests
import logging
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import redis
import oracledb as Oracle
from config import AVIASALES_API_TOKEN

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_popular_directions(**context):
    logging.info("Starting extraction from Aviasales API (popular directions from Moscow)")
    origins = ['MOW']
    all_directions = []
    url = "https://api.travelpayouts.com/v1/city-directions"

    for origin in origins:
        try:
            params = {
                'origin': origin,
                'currency': 'rub',
                'token': AVIASALES_API_TOKEN
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get('success') and 'data' in data:
                for dest, direction_data in data['data'].items():
                    direction = {
                        'origin': origin,
                        'destination': dest,
                        'price': direction_data.get('price', 0),
                        'airline': direction_data.get('airline', ''),
                        'flight_number': direction_data.get('flight_number', ''),
                        'departure_at': direction_data.get('departure_at', ''),
                        'return_at': direction_data.get('return_at', ''),
                        'expires_at': direction_data.get('expires_at', ''),
                        'transfers': direction_data.get('transfers', 0),
                        'extracted_at': datetime.now().isoformat(),
                        'data_source': 'aviasales_city_directions'
                    }
                    all_directions.append(direction)

                logging.info(f"Found {len(data['data'])} directions from {origin}")
            else:
                logging.warning(f"No direction data for {origin}")

            time.sleep(1)

        except Exception as e:
            logging.error(f"ERROR for {origin}: {str(e)}")
            continue

    context['ti'].xcom_push(key='extracted_directions', value=all_directions)
    return all_directions


def transform_directions(**context):
    logging.info("Starting transformation (popular directions)")

    ti = context['ti']
    raw_directions = ti.xcom_pull(task_ids='extract_popular_directions', key='extracted_directions')

    if not raw_directions:
        logging.warning("No directions to transform")
        return []

    transformed_directions = []
    for direction in raw_directions:
        try:
            transformed_direction = {
                'origin': direction['origin'],
                'destination': direction['destination'],
                'price': float(direction.get('price', 0)),
                'airline': direction.get('airline', ''),
                'flight_number': direction.get('flight_number', ''),
                'departure_at': direction.get('departure_at', ''),
                'return_at': direction.get('return_at', ''),
                'expires_at': direction.get('expires_at', ''),
                'transfers': int(direction.get('transfers', 0)),
                'is_direct': direction.get('transfers', 0) == 0,
                'popularity_category': 'popular' if float(direction.get('price', 0)) < 15000 else 'normal',
                'extracted_at': direction.get('extracted_at', ''),
                'processed_at': datetime.now().isoformat(),
            }
            transformed_directions.append(transformed_direction)
        except Exception as e:
            logging.error(f"ERROR transforming {direction.get('destination', 'unknown')}: {str(e)}")

    context['ti'].xcom_push(key='transformed_directions', value=transformed_directions)
    logging.info(f"Transformed {len(transformed_directions)} directions")
    return transformed_directions


def load_directions_to_mongodb(**context):
    logging.info("Loading directions to MongoDB")

    ti = context['ti']
    transformed_directions = ti.xcom_pull(task_ids='transform_directions', key='transformed_directions')
    if not transformed_directions:
        logging.error("Nothing to load into MongoDB")
        return

    try:
        client = MongoClient(host='mongodb-ods', port=27017, username='root', password='example', authSource='admin')
        db = client.aviasales_ods
        collection = db.popular_directions
        result = collection.insert_many(transformed_directions)
        logging.info(f"Loaded {len(result.inserted_ids)} directions into MongoDB")
        client.close()
    except Exception as e:
        logging.error(f"MongoDB ERROR: {str(e)}")
        raise


def load_directions_to_redis(**context):
    logging.info("Loading directions to Redis")

    ti = context['ti']
    transformed_directions = ti.xcom_pull(task_ids='transform_directions', key='transformed_directions')
    if not transformed_directions:
        logging.error("Nothing to load into Redis")
        return

    try:
        r = redis.Redis(host='redis-ods', port=6379, db=0)
        for direction in transformed_directions:
            redis_direction = {}
            for key, value in direction.items():
                if isinstance(value, bool):
                    redis_direction[key] = int(value)
                elif isinstance(value, (int, float)):
                    redis_direction[key] = str(value)
                elif value is None:
                    redis_direction[key] = ''
                else:
                    redis_direction[key] = value

            key = f"direction:{direction['origin']}:{direction['destination']}"
            r.hset(key, mapping=redis_direction)

        logging.info(f"Loaded {len(transformed_directions)} directions into Redis")

    except Exception as e:
        logging.error(f"Redis ERROR: {str(e)}")
        raise


def load_directions_to_oracle(**context):
    logging.info("Loading directions to Oracle")

    ti = context['ti']
    transformed_directions = ti.xcom_pull(task_ids='transform_directions', key='transformed_directions')
    if not transformed_directions:
        logging.error("Nothing to load into Oracle")
        return
    try:
        dsn = Oracle.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = Oracle.connect(user="aviasales", password="aviasales", dsn=dsn)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE popular_directions (
                    origin VARCHAR2(10),
                    destination VARCHAR2(10),
                    price NUMBER,
                    airline VARCHAR2(50),
                    flight_number VARCHAR2(50),
                    departure_at VARCHAR2(50),
                    return_at VARCHAR2(50),
                    expires_at VARCHAR2(50),
                    transfers NUMBER,
                    is_direct NUMBER,
                    popularity_category VARCHAR2(20),
                    extracted_at VARCHAR2(50),
                    processed_at VARCHAR2(50)
                )
            """)
        except Exception:
            logging.info("ℹTable popular_directions already exists")

        for direction in transformed_directions:
            cursor.execute("""
                INSERT INTO popular_directions (origin, destination, price, airline, flight_number, departure_at, 
                return_at, expires_at, transfers, is_direct, popularity_category, extracted_at, processed_at)
                VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)
            """, (
                direction['origin'], direction['destination'], direction['price'],
                direction['airline'], direction['flight_number'], direction['departure_at'],
                direction['return_at'], direction['expires_at'], direction['transfers'],
                int(direction['is_direct']), direction['popularity_category'],
                direction['extracted_at'], direction['processed_at']
            ))

        conn.commit()
        logging.info(f"Loaded {len(transformed_directions)} directions into Oracle")
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Oracle ERROR: {str(e)}")
        raise


with DAG('aviasales_popular_directions', default_args=default_args, description='ETL process for popular directions (Mongo, Redis, Oracle)',
        schedule_interval=timedelta(hours=6), catchup=False, tags=['aviasales', 'popular_directions', 'etl'],
         ) as dag:
    extract_task = PythonOperator(task_id='extract_popular_directions', python_callable=extract_popular_directions)
    transform_task = PythonOperator(task_id='transform_directions', python_callable=transform_directions)
    # load_mongo_task = PythonOperator(task_id='load_directions_to_mongodb', python_callable=load_directions_to_mongodb)
    # load_redis_task = PythonOperator(task_id='load_directions_to_redis', python_callable=load_directions_to_redis)
    load_oracle_task = PythonOperator(task_id='load_directions_to_oracle', python_callable=load_directions_to_oracle)

    extract_task >> transform_task
    # transform_task >> [load_mongo_task, load_redis_task, load_oracle_task]
    transform_task >> load_oracle_task