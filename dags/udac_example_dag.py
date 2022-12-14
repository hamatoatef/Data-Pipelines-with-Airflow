from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from operators.stage_redshift import StageToRedshiftOperator
from helpers import SqlQueries



#AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
#AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
        )

dq_checks = [
    {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
     'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM(SELECT cnt, count(*) FROM (SELECT "
        "songid, count(*) cnt FROM songs GROUP BY songid) GROUP BY cnt)",
     'expected_result': 1}
]
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id = 'redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_path = "s3://udacity-dend/log_data",
    json_path="s3://udacity-dend/log_json_path.json"  
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id = 'redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_path = "s3://udacity-dend/song_data",
    json_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    sql = SqlQueries.user_table_insert    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    sql = SqlQueries.song_table_insert     
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    sql = SqlQueries.artist_table_insert    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    sql = SqlQueries.time_table_insert   
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    dq_checks=[
        {'data_check_sql': 'select count(*) from public.songsplay where userid is null', 'expected_value': 0 },
        {'data_check_sql': 'select count(*) from public.users where first_name is null', 'expected_value': 0},
        {'data_check_sql': 'select count(*) from public.songs where title is null', 'expected_value': 0},
        {'data_check_sql': 'select count(*) from public.artists where name is null', 'expected_value': 0 },   
        {'data_check_sql': 'select count(*) from public.time where month is null', 'expected_value': 0}        
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator  >> stage_events_to_redshift
start_operator  >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table 
stage_songs_to_redshift  >> load_songplays_table

load_songplays_table  >> load_user_dimension_table
load_songplays_table  >> load_song_dimension_table
load_songplays_table  >> load_artist_dimension_table
load_songplays_table  >> load_time_dimension_table

load_user_dimension_table    >> run_quality_checks
load_song_dimension_table    >> run_quality_checks
load_artist_dimension_table  >> run_quality_checks
load_time_dimension_table    >> run_quality_checks

run_quality_checks           >> end_operator