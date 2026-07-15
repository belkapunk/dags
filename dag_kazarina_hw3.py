from datetime import datetime, timedelta
import ast
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.models import BaseOperator
from airflow.decorators import task, task_group
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests

# Конфигурация активных дней месяца для выполнения расчета
ALLOWED_DAYS = [1, 2, 5]


# --- Кастомный PostgresOperator для DDL/DML запросов ---
class CustomPostgresOperator(BaseOperator):
    """
    Оператор для исполнения SQL-запросов (INSERT, UPDATE, DELETE, TRUNCATE) в PostgreSQL.
    Поддерживает рендеринг Jinja-шаблонов для текста запроса.
    """
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    def __init__(self, sql: str, postgres_conn_id: str = 'conn_pg', **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        self.log.info(f"Исполнение SQL запроса через {self.postgres_conn_id}")
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        hook.run(self.sql)


# --- Функции макросов для Jinja ---
def get_start_of_month(ds):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    return dt.replace(day=1).strftime('%Y-%m-%d')


def get_end_of_month(ds):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    next_month = dt.replace(day=28) + timedelta(days=4)
    end_of_month = next_month.replace(day=1) - timedelta(days=1)
    return end_of_month.strftime('%Y-%m-%d')


# --- Функция для BranchPythonOperator ---
def check_execution_day(ds, **kwargs):
    """
    Проверка текущего дня месяца на соответствие разрешенным дням.
    Если день подходит, запускается ветка сбора данных, иначе — ветка пропуска.
    """
    dt = datetime.strptime(ds, '%Y-%m-%d')
    if dt.day in ALLOWED_DAYS:
        return 'extract_and_load_raw'
    return 'skip_execution'


default_args = {
    'owner': 'student',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='dag_kazarina_hw3_advanced',
        default_args=default_args,
        description='Ежедневный сбор статистики с кастомным ветвлением и оператором БД',
        schedule='@daily',
        start_date=datetime(2026, 3, 10),
        catchup=False,
        tags=['belkapunk', 'kazarina'],
        user_defined_macros={
            'start_of_month': get_start_of_month,
            'end_of_month': get_end_of_month
        }
) as dag:
    # Ветвление: проверка даты запуска
    branch_task = BranchPythonOperator(
        task_id='check_day_branch',
        python_callable=check_execution_day,
    )


    # Пустая таска-заглушка для дней, которые нужно пропустить
    @task
    def skip_execution():
        print("Текущий день не входит в список разрешенных. Пропуск выполнения.")


    @task
    def extract_and_load_raw(start_date, end_date):
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
        table_raw = "raw_stats_month_kazarina"

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


    # Применение кастомного PostgresOperator для этапа агрегации
    task_aggregate = CustomPostgresOperator(
        task_id='aggregate_data_postgres',
        postgres_conn_id='conn_pg',
        sql="""
            DELETE FROM agg_stats_month_kazarina 
            WHERE date = '{{ macros.user_defined_macros.start_of_month(ds) }}'::timestamp;

            INSERT INTO agg_stats_month_kazarina
            SELECT lti_user_id,
                attempt_type,
                COUNT(1) AS cnt_attempt,
                COUNT(attempt_type) FILTER (WHERE is_correct) AS cnt_correct,
                '{{ macros.user_defined_macros.start_of_month(ds) }}'::timestamp AS date
            FROM raw_stats_month_kazarina 
            WHERE created_at >= '{{ macros.user_defined_macros.start_of_month(ds) }}' 
              AND created_at <= '{{ macros.user_defined_macros.get_end_of_month(ds) }} 23:59:59'
            GROUP BY lti_user_id, attempt_type;
        """
    )


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


    # Определение параметров с Jinja-шаблонами
    start_month_template = "{{ macros.user_defined_macros.start_of_month(ds) }}"
    end_month_template = "{{ macros.user_defined_macros.end_of_month(ds) }}"

    task_extract = extract_and_load_raw(
        start_date=start_month_template,
        end_date=end_month_template
    )

    task_export_agg = export_agg_to_s3(
        start_date=start_month_template
    )

    task_export_raw = export_raw_to_s3(
        start_date=start_month_template,
        end_date=end_month_template
    )

    task_skip = skip_execution()

    # Построение логического графа зависимостей
    branch_task >> [task_extract, task_skip]

    # Основная ветка пайплайна после прохождения фильтра дат
    task_extract >> [task_aggregate >> task_export_agg, task_export_raw]
