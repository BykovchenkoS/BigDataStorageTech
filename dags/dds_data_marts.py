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
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def create_marts_if_not_exists():
    """Создает таблицы витрин если их нет"""
    conn = oracledb.connect(
        user="aviasales",
        password="aviasales",
        dsn="oracle-ods:1521/XEPDB1"
    )
    cursor = conn.cursor()

    marts = [
        ('MART_ROUTE_ANALYSIS', '''
            CREATE TABLE mart_route_analysis (
                route_id NUMBER,
                origin_code VARCHAR2(10),
                origin_city VARCHAR2(50),
                dest_code VARCHAR2(10),
                dest_city VARCHAR2(50),
                total_flights NUMBER,
                avg_price NUMBER(10,2),
                min_price NUMBER(10,2),
                max_price NUMBER(10,2),
                avg_duration_min NUMBER
            )
        '''),
        ('MART_AIRLINE_PERFORMANCE', '''
            CREATE TABLE mart_airline_performance (
                airline_code VARCHAR2(10),
                airline_name VARCHAR2(100),
                country VARCHAR2(50),
                flights_operated NUMBER,
                avg_ticket_price NUMBER(10,2),
                avg_flight_duration_min NUMBER,
                direct_flights_count NUMBER,
                direct_flights_percent NUMBER(5,2)
            )
        '''),
        ('MART_TEMPORAL_PATTERNS', '''
            CREATE TABLE mart_temporal_patterns (
                year NUMBER(4),
                month NUMBER(2),
                month_name VARCHAR2(20),
                day_of_week NUMBER(1),
                day_name VARCHAR2(10),
                is_weekend NUMBER(1),
                flights_count NUMBER,
                avg_price_this_period NUMBER(10,2),
                total_revenue_this_period NUMBER(12,2)
            )
        '''),
        ('MART_FLIGHT_TYPE_ANALYSIS', '''
            CREATE TABLE mart_flight_type_analysis (
                flight_type_id NUMBER,
                type_category VARCHAR2(20),
                description VARCHAR2(100),
                transfers_count NUMBER,
                total_flights NUMBER,
                percentage_of_total NUMBER(5,2),
                avg_price_for_type NUMBER(10,2),
                avg_duration_for_type NUMBER
            )
        '''),
        ('MART_PRICE_SUMMARY', '''
            CREATE TABLE mart_price_summary (
                period VARCHAR2(20),
                total_flights NUMBER,
                avg_price NUMBER(10,2),
                min_price NUMBER(10,2),
                max_price NUMBER(10,2),
                total_revenue NUMBER(12,2)
            )
        ''')
    ]
    
    for mart_name, create_sql in marts:
        try:
            # Проверяем существует ли таблица
            cursor.execute(f"SELECT COUNT(*) FROM user_tables WHERE table_name = '{mart_name}'")
            exists = cursor.fetchone()[0]
            
            if exists == 0:
                logging.info(f"Создаю таблицу {mart_name}")
                cursor.execute(create_sql)
                conn.commit()
                logging.info(f"Таблица {mart_name} создана")
            else:
                logging.info(f"Таблица {mart_name} уже существует")
                
        except Exception as e:
            if 'ORA-00955' not in str(e):  # Игнорируем "таблица уже существует"
                logging.error(f"Ошибка при создании {mart_name}: {e}")
                # Показываем проблемный SQL
                logging.error(f"Проблемный SQL: {create_sql[:100]}...")
    
    cursor.close()
    conn.close()

def populate_data_marts():
    """Заполняет витрины данными"""
    conn = oracledb.connect(
        user="aviasales",
        password="aviasales",
        dsn="oracle-ods:1521/XEPDB1"
    )
    cursor = conn.cursor()
    
    try:
        # 1. Очищаем и заполняем витрину маршрутов
        cursor.execute("TRUNCATE TABLE mart_route_analysis")
        cursor.execute("""
            INSERT INTO mart_route_analysis
            SELECT 
                r.route_id,
                o.airport_code AS origin_code,
                o.city AS origin_city,
                d.airport_code AS dest_code, 
                d.city AS dest_city,
                COUNT(f.flight_id) AS total_flights,
                ROUND(AVG(f.price), 2) AS avg_price,
                MIN(f.price) AS min_price,
                MAX(f.price) AS max_price,
                ROUND(AVG(f.duration), 0) AS avg_duration_min
            FROM fact_flights f
            JOIN dim_route r ON f.route_id = r.route_id
            JOIN dim_airport o ON r.origin_airport_id = o.airport_id
            JOIN dim_airport d ON r.destination_airport_id = d.airport_id
            GROUP BY r.route_id, o.airport_code, o.city, d.airport_code, d.city
            ORDER BY total_flights DESC
        """)
        logging.info("mart_route_analysis заполнена")
        
        # 2. Очищаем и заполняем витрину авиакомпаний
        cursor.execute("TRUNCATE TABLE mart_airline_performance")
        cursor.execute("""
            INSERT INTO mart_airline_performance
            SELECT 
                a.airline_bk AS airline_code,
                a.airline_name,
                a.country,
                COUNT(f.flight_id) AS flights_operated,
                ROUND(AVG(f.price), 2) AS avg_ticket_price,
                ROUND(AVG(f.duration), 0) AS avg_flight_duration_min,
                SUM(CASE WHEN ft.type_category = 'direct' THEN 1 ELSE 0 END) AS direct_flights_count,
                ROUND(SUM(CASE WHEN ft.type_category = 'direct' THEN 1 ELSE 0 END) * 100.0 / COUNT(f.flight_id), 2) AS direct_flights_percent
            FROM fact_flights f
            JOIN dim_airline a ON f.airline_sk = a.airline_sk 
            JOIN dim_flight_type ft ON f.flight_type_id = ft.flight_type_id
            WHERE a.is_current = 1
            GROUP BY a.airline_bk, a.airline_name, a.country
            ORDER BY flights_operated DESC
        """)
        logging.info("mart_airline_performance заполнена")
        
        # 3. Очищаем и заполняем временные паттерны
        cursor.execute("TRUNCATE TABLE mart_temporal_patterns")
        cursor.execute("""
            INSERT INTO mart_temporal_patterns
            SELECT 
                d.year,
                d.month,
                d.month_name,
                d.day_of_week,
                d.day_name,
                d.is_weekend,
                COUNT(f.flight_id) AS flights_count,
                ROUND(AVG(f.price), 2) AS avg_price_this_period,
                SUM(f.price) AS total_revenue_this_period
            FROM fact_flights f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.year, d.month, d.month_name, d.day_of_week, d.day_name, d.is_weekend
            ORDER BY d.year, d.month, d.day_of_week
        """)
        logging.info("mart_temporal_patterns заполнена")
        
        # 4. Очищаем и заполняем типы перелетов
        cursor.execute("TRUNCATE TABLE mart_flight_type_analysis")
        cursor.execute("""
            INSERT INTO mart_flight_type_analysis
            SELECT 
                ft.flight_type_id,
                ft.type_category,
                ft.description,
                ft.transfers_count,
                COUNT(f.flight_id) AS total_flights,
                ROUND(COUNT(f.flight_id) * 100.0 / (SELECT COUNT(*) FROM fact_flights), 2) AS percentage_of_total,
                ROUND(AVG(f.price), 2) AS avg_price_for_type,
                ROUND(AVG(f.duration), 0) AS avg_duration_for_type
            FROM fact_flights f
            JOIN dim_flight_type ft ON f.flight_type_id = ft.flight_type_id
            GROUP BY ft.flight_type_id, ft.type_category, ft.description, ft.transfers_count
            ORDER BY total_flights DESC
        """)
        logging.info("mart_flight_type_analysis заполнена")
        
        # 5. Очищаем и заполняем сводку по ценам
        cursor.execute("TRUNCATE TABLE mart_price_summary")
        cursor.execute("""
            INSERT INTO mart_price_summary
            SELECT 
                'current_month' AS period,
                COUNT(*) AS total_flights,
                ROUND(AVG(price), 2) AS avg_price,
                ROUND(MIN(price), 2) AS min_price,
                ROUND(MAX(price), 2) AS max_price,
                SUM(price) AS total_revenue
            FROM fact_flights f
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE d.month = EXTRACT(MONTH FROM SYSDATE) 
              AND d.year = EXTRACT(YEAR FROM SYSDATE)
        """)
        logging.info("mart_price_summary заполнена")
        
        conn.commit()
        logging.info("Все витрины успешно заполнены данными!")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при заполнении витрин: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def validate_marts():
    """Проверяет заполнение витрин"""
    conn = oracledb.connect(
        user="aviasales",
        password="aviasales", 
        dsn="oracle-ods:1521/XEPDB1"
    )
    cursor = conn.cursor()
    
    marts = [
        ('MART_ROUTE_ANALYSIS', 'SELECT COUNT(*) FROM mart_route_analysis'),
        ('MART_AIRLINE_PERFORMANCE', 'SELECT COUNT(*) FROM mart_airline_performance'),
        ('MART_TEMPORAL_PATTERNS', 'SELECT COUNT(*) FROM mart_temporal_patterns'),
        ('MART_FLIGHT_TYPE_ANALYSIS', 'SELECT COUNT(*) FROM mart_flight_type_analysis'),
        ('MART_PRICE_SUMMARY', 'SELECT COUNT(*) FROM mart_price_summary')
    ]
    
    for mart_name, query in marts:
        cursor.execute(query)
        count = cursor.fetchone()[0]
        logging.info(f"{mart_name}: {count} записей")
        
        if count == 0:
            raise ValueError(f"Витрина {mart_name} пустая!")
    
    cursor.close()
    conn.close()

with DAG(
    'dds_data_marts',
    default_args=default_args,
    description='Создание и заполнение витрин данных',
    schedule_interval='0 2 * * *',  # Ежедневно в 2:00
    catchup=False,
    tags=['aviasales', 'dds', 'data_marts']
) as dag:
    
    # 1. Создаем таблицы если их нет
    create_tables = PythonOperator(
        task_id='create_marts_if_not_exists',
        python_callable=create_marts_if_not_exists
    )
    
    # 2. Заполняем витрины данными
    populate_marts = PythonOperator(
        task_id='populate_data_marts',
        python_callable=populate_data_marts
    )
    
    # 3. Валидируем результат
    validate_marts_task = PythonOperator(
        task_id='validate_created_marts',
        python_callable=validate_marts
    )
    
    # Определяем порядок
    create_tables >> populate_marts >> validate_marts_task