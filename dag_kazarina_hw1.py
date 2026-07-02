from datetime import datetime, timedelta
import ast
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests

# 1. Базовые настройки
default_args = {
    'owner': 'student',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# 2. Объявляем DAG с еженедельным расписанием
with DAG(
        dag_id='dag_kazarina_hw1',
        default_args=default_args,
        description='Еженедельный сбор статистики, агрегация и экспорт в S3',
        schedule_interval='@weekly',  # Запуск раз в неделю
        start_date=datetime(2026, 3, 10),  # Дата старта
        catchup=False
) as dag:
    # --- Таск 1: Сбор сырых данных из API и запись в Postgres ---
    @task
    def extract_and_load_raw(**kwargs):
        # Даты текущего периода
        start_date = kwargs['data_interval_start'].strftime('%Y-%m-%d')
        end_date = kwargs['data_interval_end'].strftime('%Y-%m-%d')

        # Запрос к API
        url = 'https://b2b.itresume.ru/api/statistics'
        payload = {
            'client': 'Skillfactory',
            'client_key': 'M2MGWS',
            'start': start_date,
            'end': end_date,
        }
        response = requests.get(url, params=payload)
        response.raise_for_status()
        data = response.json()

        pg_hook = PostgresHook(postgres_conn_id='conn_pg')
        table_raw = "raw_stats_kazarina"

        pg_hook.run(f"TRUNCATE TABLE {table_raw}")

        rows = []
        for el in data:
            p_params = ast.literal_eval(el.get('passback_params') if el.get('passback_params') else '{}')
            row = (
                el.get('lti_user_id'),
                True if el.get('is_correct') == 1 else False,
                el.get('attempt_type'),
                el.get('created_at'),
                p_params.get('oauth_consumer_key'),
                p_params.get('lis_result_sourcedid'),
                p_params.get('lis_outcome_service_url')
            )
            rows.append(row)

        columns = [
            'lti_user_id', 'is_correct', 'attempt_type', 'created_at',
            'oauth_consumer_key', 'lis_result_sourcedid', 'lis_outcome_service_url'
        ]
        pg_hook.insert_rows(table=table_raw, rows=rows, target_fields=columns)
        print(f"Загружено {len(rows)} строк в сырую таблицу.")


    # --- Таск 2: Агрегация данных в Postgres ---
    @task
    def aggregate_data(**kwargs):
        start_date = kwargs['data_interval_start'].strftime('%Y-%m-%d')

        pg_hook = PostgresHook(postgres_conn_id='conn_pg')
        table_raw = "raw_stats_kazarina"
        table_agg = "agg_stats_kazarina"

        pg_hook.run(f"TRUNCATE TABLE {table_agg}")

        # Запрос для подсчета метрик
        sql_query = f"""
            INSERT INTO {table_agg}
            SELECT lti_user_id,
                attempt_type,
                COUNT(1) AS cnt_attempt,
                COUNT(attempt_type) FILTER (WHERE is_correct) AS cnt_correct,
                '{start_date}'::timestamp AS date
            FROM {table_raw} 
            GROUP BY lti_user_id, attempt_type;
        """
        pg_hook.run(sql_query)
        print("Данные успешно сагрегированы в Postgres.")


    # --- Таск 3: Экспорт агрегированных данных в S3 ---
    @task
    def export_agg_to_s3(**kwargs):
        start_date = kwargs['data_interval_start'].strftime('%Y-%m-%d')

        pg_hook = PostgresHook(postgres_conn_id='conn_pg')
        df = pg_hook.get_pandas_df(sql=f"SELECT * FROM agg_stats_kazarina")

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='conn_s3')
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=f"kazarina_agg_{start_date}.csv",
            bucket_name='kazarina',
            replace=True
        )
        print("Агрегированные данные отправлены в S3.")


    # --- Таск 4 (Задание с *): Экспорт сырых данных в S3 ---
    @task
    def export_raw_to_s3(**kwargs):
        start_date = kwargs['data_interval_start'].strftime('%Y-%m-%d')

        pg_hook = PostgresHook(postgres_conn_id='conn_pg')
        df = pg_hook.get_pandas_df(sql=f"SELECT * FROM raw_stats_kazarina")

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='conn_s3')
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=f"kazarina_raw_data_{start_date}.csv",
            bucket_name='kazarina',  # Ваше имя бакета
            replace=True
        )
        print("Сырые данные отправлены в S3.")


    # --- 3. Настройка параллельного графа ---
    # Запускаем извлечение
    raw_data = extract_and_load_raw()

    # После извлечения граф параллельно расходится на две независимые ветки
    # Ветка 1: Агрегируем и отправляем агрегат
    agg_flow = aggregate_data() >> export_agg_to_s3()

    # Ветка 2: Просто отправляем сырые данные в S3
    raw_flow = export_raw_to_s3()

    # Связываем всё вместе: сначала raw_data, затем обе ветки одновременно!
    raw_data >> [agg_flow, raw_flow]
