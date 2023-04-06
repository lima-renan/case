from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


DAG_ID = 'create_tables_case'

dag_args = {
    'concurrency': 10,
    'max_active_runs': 1,
    'schedule_interval': "@daily",
    'catchup': False
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 6),
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'priority_weight': 10,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120)
}

'''
# postgresql access
POSTGRES_USER = 'case'
POSTGRES_PASS = 'case_pass'
POSTGRES_HOST = 'postgres'
POSTGRES_DB = 'main'
POSTGRES_PORT = 5432

SUPPORTED_SCHEMAS = ['public']
SUPPORTED_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(schemas="', '".join(SUPPORTED_SCHEMAS))

OPTIONAL_TABLE_NAMES = ''


def connection_string():
    return "postgresql://%s:%s@%s:%s/%s" % (POSTGRES_USER, POSTGRES_PASS, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)

'''

with DAG(DAG_ID, default_args=default_args, **dag_args) as dag:
    
    criar_tabelas = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres-case',
        sql=['/querys/create_tables/raw_create_base_17.sql', '/querys/create_tables/raw_create_base_18.sql', '/querys/create_tables/raw_create_base_19.sql']
    )
    
    insere_dados=PostgresOperator(
        task_id='insere_dados',
        postgres_conn_id='postgres-case',
        sql=['/querys/load_data/load_base_17.sql', '/querys/load_data/load_base_18.sql', '/querys/load_data/load_base_19.sql']
    )

    transforma_dados=PostgresOperator(
        task_id='transforma_dados',
        postgres_conn_id='postgres-case',
        sql=['/querys/transformation/staging_base_17.sql', '/querys/transformation/staging_base_18.sql', '/querys/transformation/staging_base_19.sql']
    )

    une_tabelas=PostgresOperator(
        task_id='une_dados',
        postgres_conn_id='postgres-case',
        sql='/querys/transformation/trusted_base.sql'
    )

    tabelas_finais=PostgresOperator(
        task_id='tabelas_finais',
        postgres_conn_id='postgres-case',
        sql=['/querys/final/tb_consol_vend_ano_mes.sql', '/querys/final/tb_consol_vend_linha_ano_mes.sql', '/querys/final/tb_consol_vend_marca_ano_mes.sql','/querys/final/tb_consol_vend_marca_linha.sql']
    )

    criar_tabelas >> insere_dados >> transforma_dados >> une_tabelas >> tabelas_finais