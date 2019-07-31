from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity_student',
    'start_date': datetime.now(),
    'depends_on_past': 'False',
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('Project_5',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 1
        )

#Starts the dag
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Stages log data to redshift cluster
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table ="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
)

#Stages song data to redshift cluster
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json="auto"
)

#Loads the songplays fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query = SqlQueries.songplay_table_insert
)

#Loads the users dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    sql_query = SqlQueries.user_table_insert,
    truncate = True
)

#Loads the songs dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    sql_query = SqlQueries.song_table_insert,
    truncate = True
)

#Loads the artists dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    sql_query = SqlQueries.artist_table_insert,
    truncate = True
)

#Loads the time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    sql_query = SqlQueries.time_table_insert,
    truncate = True
)

#Checks if the primary key in songs table has null values
run_quality_checks_songs = DataQualityOperator(
    task_id='Run_data_quality_checks_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table = "songs",
    pkey_col = "song_id"
)

#Checks if the primary key in artists table has null values
run_quality_checks_artists = DataQualityOperator(
    task_id='Run_data_quality_checks_artists',
    dag=dag,
    redshift_conn_id="redshift",
    table = "artists",
    pkey_col = "artist_id"
)

#Checks if the primary key in users table has null values
run_quality_checks_users = DataQualityOperator(
    task_id='Run_data_quality_checks_users',
    dag=dag,
    redshift_conn_id="redshift",
    table = "users",
    pkey_col = "user_id"
)

#Checks if the primary key in time table has null values
run_quality_checks_time = DataQualityOperator(
    task_id='Run_data_quality_checks_time',
    dag=dag,
    redshift_conn_id="redshift",
    table = "time",
    pkey_col = "start_time"
)

#Ends the dag
end_operator = DummyOperator(task_id='End_execution',  dag=dag)

#Sets task dependencies
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> run_quality_checks_songs >> end_operator
load_songplays_table >> load_user_dimension_table >> run_quality_checks_users >> end_operator
load_songplays_table >> load_artist_dimension_table >> run_quality_checks_artists >> end_operator
load_songplays_table >> load_time_dimension_table >> run_quality_checks_time >> end_operator






