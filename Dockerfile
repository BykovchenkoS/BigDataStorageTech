FROM apache/airflow:2.7.3
RUN pip install --no-cache-dir requests pymongo redis oracledb