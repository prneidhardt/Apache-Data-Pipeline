from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from helpers.final_project_sql_statements import SqlQueries

redshift_conn_id = "redshift"
aws_credentials_id = "aws_credentials"
s3_bucket = "udacity-dend"
song_s3_key = "song_data"
events_s3_key = "log_data"

# Set default arguments
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

# Create DAG
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False,
    max_active_runs=1
)
def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')

    # Stage Operator
    s3_events_to_redshift = StageToRedshiftOperator(
        task_id='staging_events',
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table="staging_events",
        s3_bucket=s3_bucket,
        s3_key=events_s3_key,
        s3_format="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
    )

    s3_songs_to_redshift = StageToRedshiftOperator(
        task_id='staging_songs',
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table="staging_songs",
        s3_bucket=s3_bucket,
        s3_key=song_s3_key,
        s3_format="JSON 'auto'"
    )

    # Fact Operator
    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        redshift_conn_id=redshift_conn_id,
        table="songplays",
        sql_stmt=SqlQueries.songplay_table_insert
    )

    # Dimension Operators
    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        redshift_conn_id=redshift_conn_id,
        table="users",
        sql_stmt=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dim_table',
        redshift_conn_id=redshift_conn_id,
        table="songs",
        sql_stmt=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        redshift_conn_id=redshift_conn_id,
        table="artists",
        sql_stmt=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dim_table',
        redshift_conn_id=redshift_conn_id,
        table="time",
        sql_stmt=SqlQueries.time_table_insert
    )

    # Data Quality Operator
    run_quality_checks = DataQualityOperator(
        task_id='data_quality_checks',
        redshift_conn_id=redshift_conn_id,
        test_cases=[
            {'check_sql': "SELECT COUNT(*) FROM users WHERE user_id IS NULL", 
             'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id IS NULL", 
             'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM artists WHERE artist_id IS NULL", 
             'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 
             'expected_result': 0},
        ]
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    # Define dependencies
    start_operator >> [s3_events_to_redshift, s3_songs_to_redshift]
    [s3_events_to_redshift, s3_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]
    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()