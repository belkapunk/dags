from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.state import TaskInstanceState
from airflow.utils.session import provide_session


# --- Кастомный сенсор для проверки нескольких таблиц ---
class MultiTableSqlSensor(BaseSensorOperator):
    """
    Сенсор проверяет наличие данных в списке таблиц PostgreSQL.
    Переходит в статус reschedule, если хотя бы одна таблица пуста.
    """
    template_fields = ('tables',)

    def __init__(self, tables: list, postgres_conn_id: str = 'conn_pg', **kwargs):
        # Настройка режима reschedule по умолчанию
        kwargs['mode'] = kwargs.get('mode', 'reschedule')
        kwargs['poke_interval'] = kwargs.get('poke_interval', 60)
        super().__init__(**kwargs)
        self.tables = tables
        self.postgres_conn_id = postgres_conn_id

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        for table in self.tables:
            self.log.info(f"Проверка наличия данных в таблице: {table}")
            sql = f"SELECT 1 FROM {table} LIMIT 1;"
            records = hook.get_records(sql)

            if not records:
                self.log.info(f"Таблица {table} не заполнена. Ожидание.")
                return False

        self.log.info("Все таблицы содержат данные.")
        return True


# --- Кастомный сенсор для проверки нескольких внешних задач ---
class MultiExternalTaskSensor(BaseSensorOperator):
    """
    Сенсор ожидает успешного завершения нескольких задач в разных внешних DAG.
    Принимает список словарей с конфигурацией внешних тасок.
    """

    def __init__(self, external_tasks: list, **kwargs):
        kwargs['mode'] = kwargs.get('mode', 'reschedule')
        kwargs['poke_interval'] = kwargs.get('poke_interval', 60)
        super().__init__(**kwargs)
        self.external_tasks = external_tasks

    @provide_session
    def poke(self, context, session=None):
        # Использование логической даты текущего запуска для поиска внешних запусков
        execution_date = context['execution_date']

        for task_meta in self.external_tasks:
            external_dag_id = task_meta['dag_id']
            external_task_id = task_meta['task_id']

            self.log.info(f"Проверка статуса {external_dag_id}.{external_task_id}")

            # Поиск состояния конкретного TaskInstance в базе данных Airflow
            ti = session.query(context['ti'].__class__).filter(
                context['ti'].__class__.dag_id == external_dag_id,
                context['ti'].__class__.task_id == external_task_id,
                context['ti'].__class__.execution_date == execution_date
            ).first()

            if not ti or ti.state != TaskInstanceState.SUCCESS:
                self.log.info(f"Задача {external_dag_id}.{external_task_id} еще не завершена успешно.")
                return False

        self.log.info("Все внешние задачи успешно завершены.")
        return True


default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='dag_kazarina_hw4_sensors',
        default_args=default_args,
        description='Пайплайн с кастомными сенсорами множественной проверки',
        schedule='@daily',
        start_date=datetime(2026, 3, 10),
        catchup=False,
        tags=['belkapunk', 'kazarina']
) as dag:
    # 1. Пример использования SQL сенсора для нескольких таблиц
    wait_for_db_tables = MultiTableSqlSensor(
        task_id='wait_for_postgres_data',
        postgres_conn_id='conn_pg',
        tables=['raw_stats_month_kazarina', 'agg_stats_month_kazarina']
    )

    # 2. Пример использования внешнего сенсора для нескольких тасок (две звездочки)
    wait_for_external_pipelines = MultiExternalTaskSensor(
        task_id='wait_for_external_dags',
        external_tasks=[
            {'dag_id': 'dag_kazarina_hw1', 'task_id': 'export_agg_to_s3'},
            {'dag_id': 'dag_kazarina_hw2_jinja', 'task_id': 'export_raw_to_s3'}
        ]
    )


    @task
    def final_pipeline_action():
        print("Все проверки успешно пройдены. Запуск финального процесса.")


    # Логическая цепочка: ждем таблицы и внешние даги, затем выполняем действие
    [wait_for_db_tables, wait_for_external_pipelines] >> final_pipeline_action()
