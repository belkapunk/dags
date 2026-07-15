from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.models import BaseOperator

try:
    from airflow.sdk.task import task
except ImportError:
    from airflow.decorators import task

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# Конфигурационный словарь для динамического построения пайплайнов
config = [
    {
        'table_name': 'test_kazarina',
        'table_ddl': 'CREATE TABLE IF NOT EXISTS test_kazarina (id BIGINT, category VARCHAR(100), amount NUMERIC, date TIMESTAMP);',
        'table_dml': """
            DELETE FROM test_kazarina WHERE date = '{{ macros.user_defined_macros.start_of_month(ds) }}'::timestamp;
            INSERT INTO test_kazarina (id, category, amount, date)
            SELECT lti_user_id::bigint, attempt_type, COUNT(1), '{{ macros.user_defined_macros.start_of_month(ds) }}'::timestamp
            FROM raw_stats_month_kazarina
            WHERE created_at >= '{{ macros.user_defined_macros.start_of_month(ds) }}'
              AND created_at <= '{{ macros.user_defined_macros.end_of_month(ds) }} 23:59:59'
            GROUP BY lti_user_id, attempt_type;
        """,
        'need_to_export': True,
    },
    {
        'table_name': 'test_2_kazarina',
        'table_ddl': 'CREATE TABLE IF NOT EXISTS test_2_kazarina (id BIGINT, category VARCHAR(100), amount NUMERIC, date TIMESTAMP);',
        'table_dml': """
            DELETE FROM test_2_kazarina WHERE date = '{{ macros.user_defined_macros.start_of_month(ds) }}'::timestamp;
            INSERT INTO test_2_kazarina (id, category, amount, date)
            SELECT lti_user_id::bigint, attempt_type, COUNT(1) * 2, '{{ macros.user_defined_macros.start_of_month(ds) }}'::timestamp
            FROM raw_stats_month_kazarina
            WHERE created_at >= '{{ macros.user_defined_macros.start_of_month(ds) }}'
              AND created_at <= '{{ macros.user_defined_macros.end_of_month(ds) }} 23:59:59'
            GROUP BY lti_user_id, attempt_type;
        """,
        'need_to_export': False,
    }
]


class CustomPostgresOperator(BaseOperator):
    """Кастомный оператор для исполнения DDL и DML запросов в Postgres"""
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#f4f4f4'

    def __init__(self, sql: str, postgres_conn_id: str = 'conn_pg', **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        hook.run(self.sql)


def get_start_of_month(ds):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    return dt.replace(day=1).strftime('%Y-%m-%d')

def get_end_of_month(ds):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    next_month = dt.replace(day=28) + timedelta(days=4)
    end_of_month = next_month.replace(day=1) - timedelta(days=1)
    return end_of_month.strftime('%Y-%m-%d')


@task(task_id='export_table_to_s3')
def export_to_s3_task(table_name, start_date):
    """Опциональная выгрузка результатов в S3 хранилище"""
    pg_hook = PostgresHook(postgres_conn_id='conn_pg')
    sql = f"SELECT * FROM {table_name} WHERE date = '{start_date}'::timestamp"
    df = pg_hook.get_pandas_df(sql=sql)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_hook = S3Hook(aws_conn_id='conn_s3')
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f"kazarina_final_{table_name}_{start_date}.csv",
        bucket_name='kazarina',
        replace=True
    )
    print(f"Данные таблицы {table_name} экспортированы в S3.")


default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='dag_kazarina_final_project',
        default_args=default_args,
        description='Итоговый проект: динамический расчет агрегатов на основе конфигурации',
        schedule='@daily',
        start_date=datetime(2026, 3, 10),
        catchup=False,
        tags=['belkapunk', 'kazarina', 'final'],
        user_defined_macros={
            'start_of_month': get_start_of_month,
            'end_of_month': get_end_of_month
        }
) as dag:

    # Цикл по элементам конфигурации для динамического построения графа задач
    for target in config:
        t_name = target['table_name']

        # 1. Задача создания таблицы
        create_table = CustomPostgresOperator(
            task_id=f'create_table_{t_name}',
            sql=target['table_ddl']
        )

        # 2. Задача наполнения таблицы (включает логику DELETE для идемпотентности)
        load_data = CustomPostgresOperator(
            task_id=f'load_data_{t_name}',
            sql=target['table_dml']
        )

        # Связываем создание структуры и наполнение данными
        create_table >> load_data

        # 3. Опциональный этап выгрузки в объектное хранилище
        if target['need_to_export']:
            export_s3 = export_to_s3_task(
                table_name=t_name,
                start_date="{{ macros.user_defined_macros.start_of_month(ds) }}"
            )
            # Добавляем экспорт в цепочку после успешного наполнения таблицы
            load_data >> export_s3
