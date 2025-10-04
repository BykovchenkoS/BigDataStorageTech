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


def extract_price_trends(**context):
    logging.info("Starting extraction from Aviasales API (price trends)")
    API_TOKEN = "b7721379bd50af994b608f3216348cb4"
    routes = [('MOW', 'LED')]
    depart_month = '2025-10'
    all_prices = []
    url = "https://api.travelpayouts.com/v1/prices/calendar"

    for origin, destination in routes:
        try:
            params = {
                'origin': origin,
                'destination': destination,
                'depart_date': depart_month,
                'calendar_type': 'departure_date',
                'currency': 'rub',
                'token': API_TOKEN
            }
            response = requests.get(url, params=params, timeout=30)

            if not response.text.strip():
                logging.warning(f"Empty response for {origin}-{destination}")
                continue

            response.raise_for_status()
            data = response.json()

            if data.get('success') and 'data' in data:
                for date_str, price_data in data['data'].items():
                    ticket = {
                        'ticket_id': f"{price_data.get('airline','')}_{price_data.get('flight_number','')}_{origin}_{destination}_{date_str}",
                        'origin': origin,
                        'destination': destination,
                        'departure_at': price_data.get('departure_at', ''),
                        'return_at': price_data.get('return_at', ''),
                        'price': price_data.get('price', 0),
                        'airline': price_data.get('airline', ''),
                        'flight_number': price_data.get('flight_number', ''),
                        'transfers': price_data.get('transfers', 0),
                        'expires_at': price_data.get('expires_at', ''),
                        'extracted_at': datetime.now().isoformat(),
                        'route': f"{origin}-{destination}",
                        'date': date_str,
                        'data_source': 'aviasales_price_trends'
                    }
                    all_prices.append(ticket)
                logging.info(f"Found {len(data['data'])} prices for route {origin}-{destination}")
            else:
                logging.warning(f"No price data for {origin}-{destination}")

            time.sleep(0.2)

        except Exception as e:
            logging.error(f"ERROR for {origin}-{destination}: {str(e)}")
            continue

    context['ti'].xcom_push(key='extracted_prices', value=all_prices)
    return all_prices


def transform_price_trends(**context):
    logging.info("Starting transformation (price trends)")

    ti = context['ti']
    raw_prices = ti.xcom_pull(task_ids='extract_price_trends', key='extracted_prices')
    if not raw_prices:
        logging.warning("No price trends to transform")
        return []

    transformed_prices = []
    for price in raw_prices:
        try:
            transformed_price = {
                'ticket_id': price['ticket_id'],
                'origin': price['origin'],
                'destination': price['destination'],
                'departure_at': price['departure_at'],
                'return_at': price['return_at'],
                'price': float(price.get('price', 0)),
                'airline': price.get('airline', ''),
                'flight_number': price.get('flight_number', ''),
                'transfers': int(price.get('transfers', 0)),
                'is_direct': price.get('transfers', 0) == 0,
                'expires_at': price.get('expires_at', ''),
                'trend_category': 'cheap' if float(price.get('price',0)) < 10000 else 'normal',
                'processed_at': datetime.now().isoformat(),
                'route': price.get('route','')
            }
            transformed_prices.append(transformed_price)
        except Exception as e:
            logging.error(f"ERROR transforming {price.get('ticket_id','unknown')}: {str(e)}")

    context['ti'].xcom_push(key='transformed_prices', value=transformed_prices)
    logging.info(f"Transformed {len(transformed_prices)} price records")
    return transformed_prices


def load_prices_to_mongodb(**context):
    logging.info("Loading price trends to MongoDB")
    ti = context['ti']
    transformed_prices = ti.xcom_pull(task_ids='transform_price_trends', key='transformed_prices')
    if not transformed_prices:
        logging.error("Nothing to load into MongoDB")
        return

    try:
        client = MongoClient(host='mongodb-ods', port=27017, username='root', password='example', authSource='admin')
        db = client.aviasales_ods
        collection = db.price_trends
        result = collection.insert_many(transformed_prices)
        logging.info(f"Loaded {len(result.inserted_ids)} price records into MongoDB")
        client.close()
    except Exception as e:
        logging.error(f"MongoDB ERROR: {str(e)}")
        raise


def load_prices_to_redis(**context):
    logging.info("Loading price trends to Redis")
    ti = context['ti']
    transformed_prices = ti.xcom_pull(task_ids='transform_price_trends', key='transformed_prices')
    if not transformed_prices:
        logging.error("Nothing to load into Redis")
        return

    try:
        r = redis.Redis(host='redis-ods', port=6379, db=0)
        for price in transformed_prices:
            redis_price = {}
            for key, value in price.items():
                if isinstance(value, bool):
                    redis_price[key] = int(value)
                elif isinstance(value, (int, float)):
                    redis_price[key] = str(value)
                elif value is None:
                    redis_price[key] = ''
                else:
                    redis_price[key] = value

            key = f"price_trend:{price['ticket_id']}"
            r.hset(key, mapping=redis_price)

        logging.info(f"Loaded {len(transformed_prices)} price records into Redis")

    except Exception as e:
        logging.error(f"Redis ERROR: {str(e)}")
        raise


def load_prices_to_oracle(**context):
    logging.info("Loading price trends to Oracle")
    ti = context['ti']
    transformed_prices = ti.xcom_pull(task_ids='transform_price_trends', key='transformed_prices')
    if not transformed_prices:
        logging.error("Nothing to load into Oracle")
        return

    try:
        dsn = Oracle.makedsn("oracle-ods", 1521, service_name="XEPDB1")
        conn = Oracle.connect(user="system", password="oracle", dsn=dsn)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                CREATE TABLE price_trends (
                    ticket_id VARCHAR2(100),
                    origin VARCHAR2(10),
                    destination VARCHAR2(10),
                    departure_at VARCHAR2(50),
                    return_at VARCHAR2(50),
                    price NUMBER,
                    airline VARCHAR2(50),
                    flight_number VARCHAR2(50),
                    transfers NUMBER,
                    is_direct NUMBER,
                    expires_at VARCHAR2(50),
                    trend_category VARCHAR2(20),
                    processed_at VARCHAR2(50),
                    route VARCHAR2(20)
                )
            """)

        except Exception:
            logging.info("Table price_trends already exists")

        for price in transformed_prices:
            cursor.execute("""
                INSERT INTO price_trends (ticket_id, origin, destination, departure_at, return_at, price, airline, 
                flight_number, transfers, is_direct, expires_at, trend_category, processed_at, route)
                VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)
            """, (
                price['ticket_id'], price['origin'], price['destination'], price['departure_at'], price['return_at'],
                price['price'], price['airline'], price['flight_number'], price['transfers'], int(price['is_direct']),
                price['expires_at'], price['trend_category'], price['processed_at'], price['route']
            ))

        conn.commit()
        logging.info(f"Loaded {len(transformed_prices)} price records into Oracle")
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Oracle ERROR: {str(e)}")
        raise


with DAG('aviasales_price_trends',default_args=default_args, description='ETL process for flight price trends (Mongo, Redis, Oracle)',
         schedule_interval=timedelta(hours=6), catchup=False, tags=['aviasales', 'price_trends', 'etl'],
) as dag:
    extract_task = PythonOperator(task_id='extract_price_trends', python_callable=extract_price_trends)
    transform_task = PythonOperator(task_id='transform_price_trends', python_callable=transform_price_trends)
    load_mongo_task = PythonOperator(task_id='load_prices_to_mongodb', python_callable=load_prices_to_mongodb)
    load_redis_task = PythonOperator(task_id='load_prices_to_redis', python_callable=load_prices_to_redis)
    load_oracle_task = PythonOperator(task_id='load_prices_to_oracle', python_callable=load_prices_to_oracle)

    extract_task >> transform_task
    transform_task >> [load_mongo_task, load_redis_task, load_oracle_task]
