from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.models import baseoperator

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 12, 1),
    'depends_on_past':False,
    'email_on_retry':False,
    'retries':3,
    'retry_delay': timedelta(seconds=5),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_events_table = PostgresOperator(
        task_id="create_staging_events_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_STAGING_EVENTS_TABLE_SQL
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="rvi-airflow",
        copy_json_option="auto",
        s3_key="rvi-airflow/log-data/{execution_date.year}/{execution_date.month}/{ds}-events.json"
    )

    Stage_events = stage_events_to_redshift

    create_songs_table = PostgresOperator(
        task_id="create_staging_song_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_STAGING_SONGS_TABLE_SQL
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="rvi-airflow",
        copy_json_option="auto",
        s3_key="rvi-airflow/song-data/A/A/A"

    )

    Stage_songs = stage_songs_to_redshift


    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    start_operator >> create_events_table >> Stage_events
    start_operator >> create_songs_table >> Stage_songs

final_project_dag = final_project()
