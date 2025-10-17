from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
from pymongo import MongoClient
import redis
import oracledb
import matplotlib.pyplot as plt
import io
import base64
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def generate_test_data(num_records=500):
    test_data = []
    for i in range(num_records):
        ticket = {
            'ticket_id': f'test_ticket_{i}',
            'origin': 'MOW',
            'destination': 'LED',
            'price': 5000 + (i % 10) * 1000,
            'airline': f'airline_{i % 5}',
            'flight_number': f'FL{i}',
            'transfers': i % 3,
            'is_direct': i % 3 == 0,
            'departure_at': '2025-10-15',
            'extracted_at': datetime.now().isoformat()
        }
        test_data.append(ticket)
    return test_data


def benchmark_mongodb():
    test_data = generate_test_data(500)
    client = MongoClient('mongodb-ods', 27017, username='root', password='example', authSource='admin')
    db = client.aviasales_ods
    collection = db.benchmark

    # тест записи
    start_time = time.time()
    result = collection.insert_many(test_data)
    write_time = time.time() - start_time

    # тест чтения
    start_time = time.time()
    results = list(collection.find({"price": {"$lt": 8000}}))
    read_time = time.time() - start_time

    # очистка
    collection.delete_many({})
    client.close()

    print(f"MongoDB - Write: {write_time:.3f}s, Read: {read_time:.3f}s, Found: {len(results)} records")
    return {'write_time': write_time, 'read_time': read_time, 'records': len(results)}


def benchmark_redis():
    test_data = generate_test_data(500)
    r = redis.Redis(host='redis-ods', port=6379, db=0, decode_responses=True)

    # тест записи
    start_time = time.time()
    for ticket in test_data:
        key = f"benchmark:{ticket['ticket_id']}"
        redis_data = {k: str(v) for k, v in ticket.items()}
        r.hset(key, mapping=redis_data)
    write_time = time.time() - start_time

    # тест чтения
    start_time = time.time()
    count = 0
    for ticket in test_data[:100]:
        key = f"benchmark:{ticket['ticket_id']}"
        data = r.hgetall(key)
        if data:
            count += 1
    read_time = time.time() - start_time

    # очистка
    for ticket in test_data:
        key = f"benchmark:{ticket['ticket_id']}"
        r.delete(key)

    print(f"Redis - Write: {write_time:.3f}s, Read: {read_time:.3f}s, Found: {count} records")
    return {'write_time': write_time, 'read_time': read_time, 'records': count}


def benchmark_oracle():
    test_data = generate_test_data(500)
    dsn = oracledb.makedsn("oracle-ods", 1521, service_name="XEPDB1")
    conn = oracledb.connect(user="system", password="oracle", dsn=dsn)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE benchmark_tickets (
                ticket_id VARCHAR2(100),
                origin VARCHAR2(10),
                destination VARCHAR2(10),
                price NUMBER,
                airline VARCHAR2(50),
                flight_number VARCHAR2(50),
                transfers NUMBER,
                is_direct NUMBER,
                departure_at VARCHAR2(50),
                extracted_at VARCHAR2(50)
            )
        """)
        print("Created benchmark_tickets table in Oracle")
    except Exception as e:
        print("Table benchmark_tickets already exists")

    # тест записи
    start_time = time.time()
    for ticket in test_data:
        cursor.execute("""
            INSERT INTO benchmark_tickets VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)
        """, (
            ticket['ticket_id'], ticket['origin'], ticket['destination'],
            ticket['price'], ticket['airline'], ticket['flight_number'],
            ticket['transfers'], 1 if ticket['is_direct'] else 0,
            ticket['departure_at'], ticket['extracted_at']
        ))
    conn.commit()
    write_time = time.time() - start_time

    # тест чтения
    start_time = time.time()
    cursor.execute("SELECT COUNT(*) FROM benchmark_tickets WHERE price < 8000")
    result = cursor.fetchone()
    read_time = time.time() - start_time

    # очистка
    try:
        cursor.execute("DROP TABLE benchmark_tickets")
        conn.commit()
    except:
        pass

    cursor.close()
    conn.close()

    print(f"Oracle - Write: {write_time:.3f}s, Read: {read_time:.3f}s, Found: {result[0]} records")
    return {'write_time': write_time, 'read_time': read_time, 'records': result[0]}


def draw_graph(mongo_result, redis_result, oracle_result):
    try:
        databases = ['MongoDB', 'Redis', 'Oracle']
        write_times = [mongo_result['write_time'], redis_result['write_time'], oracle_result['write_time']]
        read_times = [mongo_result['read_time'], redis_result['read_time'], oracle_result['read_time']]
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        # запись
        bars1 = ax1.bar(databases, write_times, color=['green', 'red', 'blue'], alpha=0.8)
        ax1.set_title('Время записи 500 записей', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Секунды', fontsize=12)
        ax1.tick_params(axis='x', rotation=45)
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width() / 2., height,
                     f'{height:.3f}s',
                     ha='center', va='bottom', fontweight='bold')

        # чтение
        bars2 = ax2.bar(databases, read_times, color=['green', 'red', 'blue'], alpha=0.8)
        ax2.set_title('Время чтения', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Секунды', fontsize=12)
        ax2.tick_params(axis='x', rotation=45)
        for bar in bars2:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width() / 2., height,
                     f'{height:.3f}s',
                     ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        os.makedirs('/tmp/benchmark_charts', exist_ok=True)
        chart_path = '/tmp/benchmark_charts/performance_chart.png'
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close()

        print(f"Столбчатая диаграмма сохранена {chart_path}")
        return chart_path

    except Exception as e:
        print(f"Ошибка при создании графика {e}")
        return None


def analyze_results(**context):
    ti = context['ti']
    mongo_result = ti.xcom_pull(task_ids='benchmark_mongodb')
    redis_result = ti.xcom_pull(task_ids='benchmark_redis')
    oracle_result = ti.xcom_pull(task_ids='benchmark_oracle')

    print("=" * 50)
    print("Результаты сравнительного анализа")
    print("=" * 50)
    print(f"MongoDB:")
    print(f"Запись 500 записей {mongo_result['write_time']:.3f} сек")
    print(f"Чтение по условию {mongo_result['read_time']:.3f} сек")
    print(f"Найдено записей {mongo_result['records']}")
    print(f"Redis:")
    print(f"Запись 500 записей {redis_result['write_time']:.3f} сек")
    print(f"Чтение 100 записей {redis_result['read_time']:.3f} сек")
    print(f"Найдено записей {redis_result['records']}")
    print(f"Oracle:")
    print(f"Запись 500 записей {oracle_result['write_time']:.3f} сек")
    print(f"Чтение по условию {oracle_result['read_time']:.3f} сек")
    print(f"Найдено записей {oracle_result['records']}")

    chart_path = draw_graph(mongo_result, redis_result, oracle_result)
    if chart_path:
        print(f"Диаграмма успешно создана {chart_path}")
    else:
        print("Не удалось создать диаграмму")

    print("=" * 50)
    fastest_write = min(mongo_result['write_time'], redis_result['write_time'], oracle_result['write_time'])
    fastest_read = min(mongo_result['read_time'], redis_result['read_time'], oracle_result['read_time'])

    print(f"Лучшие результаты:")
    if mongo_result['write_time'] == fastest_write:
        print("Самая быстрая запись в MongoDB")
    elif redis_result['write_time'] == fastest_write:
        print("Самая быстрая запись в Redis")
    else:
        print("Самая быстрая запись в Oracle")

    if mongo_result['read_time'] == fastest_read:
        print("Самое быстрое чтение в MongoDB")
    elif redis_result['read_time'] == fastest_read:
        print("Самое быстрое чтение в Redis")
    else:
        print("Самое быстрое чтение в Oracle")


with DAG('benchmark_databases', default_args=default_args, description='DB performance benchmark comparison',
         schedule_interval=None, catchup=False, tags=['benchmark', 'analysis'],
) as dag:
    benchmark_mongo = PythonOperator(task_id='benchmark_mongodb', python_callable=benchmark_mongodb)
    benchmark_redis = PythonOperator(task_id='benchmark_redis', python_callable=benchmark_redis)
    benchmark_oracle = PythonOperator(task_id='benchmark_oracle', python_callable=benchmark_oracle)
    analyze = PythonOperator(task_id='analyze_results', python_callable=analyze_results, provide_context=True)

    [benchmark_mongo, benchmark_redis, benchmark_oracle] >> analyze