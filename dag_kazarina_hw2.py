from datetime import datetime, timedelta
import ast
import pandas as pd
from io import StringIO

from airflow import DAG

try:
    from airflow.sdk.task import task
except ImportError:
    from airflow.decorators import task

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests


# Расчет временных интервалов для Jinja-шаблонов
def get_start_of_month(ds):
    """Определение первого дня месяца для переданной даты"""
    dt = datetime.strptime(ds, '%Y-%m-%d')
    return dt.replace(day=1).strftime('%Y-%m-%d')


def get_end_of_month(ds):
    """Определение последнего дня месяца для переданной даты"""
    dt = datetime.strptime(ds, '%Y-%m-%d')
    next_month = dt.replace(day=28) + timedelta(days=4)
    end_of_month = next_month.replace(day=1) - timedelta(days=1)
    return end_of_month.strftime('%Y-%m-%d')


default_args = {
    'owner': 'student',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='dag_kazarina_hw2_jinja',
        default_args=default_args,
        description='Ежедневный сбор статистики за месяц с Jinja и экспорт в S3',
        schedule='@daily',
        start_date=datetime(2026, 3, 10),
        catchup=False,
        tags=['belkapunk', 'kazarina'],
        user_defined_macros={
            'start_of_month': get_start_of_month,
            'end_of_month': get_end_of_month
        }
) as dag:
    @task
    def extract_and_load_raw(start_date, end_date):
        url = 'https://itresume.ru'
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
        table_raw = "raw_stats_month_kazarina"

        # Перезапись данных за текущий расчетный период
        delete_query = f"""
            DELETE FROM {table_raw} 
            WHERE created_at >= '{start_date}' AND created_at <= '{end_date} 23:59:59';
        """
        pg_hook.run(delete_query)

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

        if rows:
            pg_hook.insert_rows(table=table_raw, rows=rows, target_fields=columns)
        print(f"Загружено {len(rows)} строк.")


    @task
    def aggregate_data(start_date, end_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_pg')
        table_raw = "raw_stats_month_kazarina"
        table_agg = "agg_stats_month_kazarina"

        # Очистка старых агрегированных данных перед обновлением
        pg_hook.run(f"DELETE FROM {table_agg} WHERE date = '{start_date}'::timestamp")

        sql_query = f"""
            INSERT INTO {table_agg}
            SELECT lti_user_id,
                attempt_type,
                COUNT(1) AS cnt_attempt,
                COUNT(attempt_type) FILTER (WHERE is_correct) AS cnt_correct,
                '{start_date}'::timestamp AS date
            FROM {table_raw} 
            WHERE created_at >= '{start_date}' AND created_at <= '{end_date} 23:59:59'
            GROUP BY lti_user_id, attempt_type;
        """
        pg_hook.run(sql_query)
        print("Данные успешно сагрегированы.")


    @task
    def export_agg_to_s3(start_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_pg')
        sql = f"SELECT * FROM agg_stats_month_kazarina WHERE date = '{start_date}'::timestamp"
        df = pg_hook.get_pandas_df(sql=sql)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='conn_s3')
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=f"kazarina_month_agg_{start_date}.csv",
            bucket_name='kazarina',
            replace=True
        )
        print("Агрегированные данные отправлены в S3.")


    @task
    def export_raw_to_s3(start_date, end_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_pg')
        sql = f"""
            SELECT * FROM raw_stats_month_kazarina 
            WHERE created_at >= '{start_date}' AND created_at <= '{end_date} 23:59:59'
        """
        df = pg_hook.get_pandas_df(sql=sql)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='conn_s3')
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=f"kazarina_month_raw_{start_date}.csv",
            bucket_name='kazarina',
            replace=True
        )
        print("Сырые данные за месяц отправлены в S3.")


    # Формирование параметров через Jinja-шаблоны и запуск тасок
    task_extract = extract_and_load_raw(
        start_date="{{ macros.user_defined_macros.start_of_month(ds) }}",
        end_date="{{ macros.user_defined_macros.end_of_month(ds) }}"
    )

    task_aggregate = aggregate_data(
        start_date="{{ macros.user_defined_macros.start_of_month(ds) }}",
        end_date="{{ macros.user_defined_macros.end_of_month(ds) }}"
    )

    task_export_agg = export_agg_to_s3(
        start_date="{{ macros.user_defined_macros.start_of_month(ds) }}"
    )

    task_export_raw = export_raw_to_s3(
        start_date="{{ macros.user_defined_macros.start_of_month(ds) }}",
        end_date="{{ macros.user_defined_macros.end_of_month(ds) }}"
    )

    # Структура пайплайна с параллельными ветками выгрузки
    task_extract >> [task_aggregate >> task_export_agg, task_export_raw]
