from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'siva_udacity',
    'start_date': datetime(2021, 7, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('siva_udac_example_dag_final3',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_events',
    s3_bucket='udacity-dend',
    s3_path='log_data',
    aws_conn_id = 'aws_credentials',
    region='us-west-2',
    copy_json_option='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_songs',
    s3_bucket='udacity-dend',
    s3_path='song_data/A/A/A',
    aws_conn_id = 'aws_credentials',
    region='us-west-2',
    copy_json_option='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='songplays',
    insert_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    insert_sql=SqlQueries.user_table_insert,
    truncate_table='YES'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    insert_sql=SqlQueries.song_table_insert,
    truncate_table='YES'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    insert_sql=SqlQueries.artist_table_insert,
    truncate_table='YES'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    insert_sql=SqlQueries.time_table_insert,
    truncate_table='YES'
)
# extra checks
dq_checks = [
    {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null",
     'expected_result': 0,
     'descr': "null values in users.userid column"},
    {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
     'expected_result': 0,
     'descr': "null values in songs.songid column"}
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['time','users', 'songs','artists'],
    redshift_conn_id='redshift',
    checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift,
                           stage_songs_to_redshift] >> \
    load_songplays_table >> [load_song_dimension_table,
                             load_user_dimension_table,
                             load_artist_dimension_table,
                             load_time_dimension_table] >> \
    run_quality_checks >> \
    end_operator