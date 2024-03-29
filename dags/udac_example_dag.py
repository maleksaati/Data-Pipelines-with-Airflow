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
    'owner': 'Malek',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region = 'us-east-2',
    table="staging_events",
    s3_bucket="s3://udacity-dend/log_data",
    json_path = "s3://udacity-dend/log_json_path.json",
    dag=dag
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region = 'us-west-2',
    table="staging_songs",
    s3_bucket="s3://udacity-dend/song_data",
    json_path = "auto",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    insertTable = SqlQueries.songplay_table_insert,
    dag=dag,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    insertTable = SqlQueries.user_table_insert,
    append_or_delte=False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    insertTable = SqlQueries.song_table_insert,
    append_or_delte=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    insertTable = SqlQueries.artist_table_insert,
    append_or_delte=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    insertTable = SqlQueries.time_table_insert,
    append_or_delte=False,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    field="playid",
    table='songplays',
    redshift_conn_id="redshift",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator \
    >> [stage_events_to_redshift,
        stage_songs_to_redshift]  \
    >> load_songplays_table\
    >> [load_time_dimension_table,
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table] \
    >> run_quality_checks \
    >> end_operator