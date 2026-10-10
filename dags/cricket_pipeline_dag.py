
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.utils.task_group import TaskGroup
# from airflow.operators.bash import BashOperator
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
# DBT_DIR = "/opt/airflow/dbt"
# CONN_ID = "snowflake_default"

# default_args = {
#     "owner": "data_engineering",
#     "depends_on_past": False,
#     "retries": 2,
#     "retry_delay": timedelta(minutes=2),
# }

# with DAG(
#     dag_id="cricket_pipeline",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="@daily",
#     catchup=False,
#     default_args=default_args,
#     tags=["medallion", "dbt", "snowflake", "raw_ingestion"],
# ) as dag:
#     with TaskGroup(group_id="raw_ingestion") as raw_ingestion:
#         init_snowflake = SQLExecuteQueryOperator(
#             task_id="init_snowflake_raw_objects",
#             conn_id=CONN_ID,
#             sql="""
#             CREATE SCHEMA IF NOT EXISTS RAW;
#             CREATE STAGE IF NOT EXISTS RAW.INGEST_STAGE;
#             CREATE FILE FORMAT IF NOT EXISTS RAW.CSV_FORMAT TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1;
#             CREATE TABLE IF NOT EXISTS RAW.CRICKET (
#     delivery_id STRING,
#     delivery_timestamp STRING,
#     inning_no STRING,
#     over_no STRING,
#     ball_in_over STRING,
#     runs_off_bat STRING,
#     extra_type STRING,
#     extra_runs STRING,
#     total_runs STRING,
#     is_wicket STRING,
#     dismissal_type STRING,
#     ball_speed_kmph STRING,
#     non_striker_id STRING,
#     shot_type STRING,
#     shot_region STRING,
#     match_id STRING,
#     match_date STRING,
#     match_format STRING,
#     tournament_name STRING,
#     season STRING,
#     match_stage STRING,
#     toss_decision STRING,
#     venue_id STRING,
#     venue_name STRING,
#     venue_city STRING,
#     venue_country STRING,
#     batting_team_id STRING,
#     batting_team_name STRING,
#     bowling_team_id STRING,
#     bowling_team_name STRING,
#     batsman_id STRING,
#     batsman_name STRING,
#     batsman_dob STRING,
#     batsman_nationality STRING,
#     batting_hand STRING,
#     batsman_role STRING,
#     batsman_email STRING,
#     batsman_phone STRING,
#     batsman_icc_rating STRING,
#     bowler_id STRING,
#     bowler_name STRING,
#     bowler_nationality STRING,
#     bowling_style STRING,
#     bowler_icc_rating STRING,
#     contract_id STRING,
#     contract_grade STRING,
#     annual_retainer_usd STRING,
#     contract_status STRING,
#     drs_review_id STRING,
#     drs_reviewed_by STRING,
#     drs_original_decision STRING,
#     drs_outcome STRING,
#     _FILE_NAME STRING,
#     _LOADED_AT TIMESTAMP_NTZ
# );
#             """,
#         )

#         put_csv = SQLExecuteQueryOperator(
#             task_id="put_csv_to_stage",
#             conn_id=CONN_ID,
#             sql="PUT file:///opt/airflow/data/cricket_data.csv @RAW.INGEST_STAGE/cricket "
#                 "AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=8",
#         )

#         copy_csv = SQLExecuteQueryOperator(
#     task_id="copy_csv_to_raw",
#     conn_id=CONN_ID,
#     sql="""
#     TRUNCATE TABLE RAW.CRICKET;

#     COPY INTO RAW.CRICKET (
#         delivery_id,
#         delivery_timestamp,
#         inning_no,
#         over_no,
#         ball_in_over,
#         runs_off_bat,
#         extra_type,
#         extra_runs,
#         total_runs,
#         is_wicket,
#         dismissal_type,
#         ball_speed_kmph,
#         non_striker_id,
#         shot_type,
#         shot_region,
#         match_id,
#         match_date,
#         match_format,
#         tournament_name,
#         season,
#         match_stage,
#         toss_decision,
#         venue_id,
#         venue_name,
#         venue_city,
#         venue_country,
#         batting_team_id,
#         batting_team_name,
#         bowling_team_id,
#         bowling_team_name,
#         batsman_id,
#         batsman_name,
#         batsnam_dob,
#         batsman_nationality,
#         batting_hand,
#         batsman_role,
#         batsman_email,
#         batsman_phone,
#         batsman_icc_rating,
#         bowler_id,
#         bowler_name,
#         bowler_nationality,
#         bowling_style,
#         bowler_icc_rating,
#         contract_id,
#         contract_grade,
#         annual_retainer_usd,
#         contract_status,
#         drs_review_id,
#         drs_reviewed_by,
#         drs_original_decision,
#         drs_outcome,
#         _FILE_NAME,
#         _LOADED_AT
#     )
#     FROM (
#         SELECT
#             t.$1, t.$2, t.$3, t.$4, t.$5, t.$6,
#             t.$7, t.$8, t.$9, t.$10, t.$11, t.$12,
#             t.$13, t.$14, t.$15, t.$16, t.$17, t.$18,
#             t.$19, t.$20, t.$21, t.$22, t.$23, t.$24,
#             t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
#             t.$31, t.$32, t.$33, t.$34, t.$35, t.$36,
#             t.$37, t.$38, t.$39, t.$40, t.$41, t.$42,
#             t.$43, t.$44, t.$45, t.$46, t.$47, t.$48,
#             t.$49, t.$50, t.$51, t.$52,
#             METADATA$FILENAME,
#             CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
#         FROM @RAW.INGEST_STAGE/cricket t
#     )
#     FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
#     ON_ERROR = 'CONTINUE';
#     """,
# )

#         init_snowflake >> put_csv
#         put_csv >> copy_csv


from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ============================================================
# CONFIGURATION
# ============================================================

CONN_ID = "snowflake_default"

CSV_FILE = "/opt/airflow/data/cricket_data.csv"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"
CONN_ID = "snowflake_default"


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id="cricket_pipeline",
    description="Cricket CSV ingestion into Snowflake RAW layer",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["medallion", "dbt", "snowflake", "raw_ingestion"],
) as dag:

    # ========================================================
    # RAW INGESTION TASK GROUP
    # ========================================================

    with TaskGroup(group_id="raw_ingestion") as raw_ingestion:

        # ----------------------------------------------------
        # TASK 1: INITIALIZE SNOWFLAKE OBJECTS
        # ----------------------------------------------------

        init_snowflake = SQLExecuteQueryOperator(
            task_id="init_snowflake_raw_objects",
            conn_id=CONN_ID,
            sql=[
                """
                CREATE SCHEMA IF NOT EXISTS RAW
                """,
                """
                CREATE STAGE IF NOT EXISTS RAW.INGEST_STAGE
                """,
                """
                CREATE FILE FORMAT IF NOT EXISTS RAW.CSV_FORMAT
                    TYPE = 'CSV'
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    SKIP_HEADER = 1
                    EMPTY_FIELD_AS_NULL = TRUE
                """,
                """
                CREATE TABLE IF NOT EXISTS RAW.CRICKET (
                    delivery_id STRING,
                    delivery_timestamp STRING,
                    inning_no STRING,
                    over_no STRING,
                    ball_in_over STRING,
                    runs_off_bat STRING,
                    extra_type STRING,
                    extra_runs STRING,
                    total_runs STRING,
                    is_wicket STRING,
                    dismissal_type STRING,
                    ball_speed_kmph STRING,
                    non_striker_id STRING,
                    shot_type STRING,
                    shot_region STRING,
                    match_id STRING,
                    match_date STRING,
                    match_format STRING,
                    tournament_name STRING,
                    season STRING,
                    match_stage STRING,
                    toss_decision STRING,
                    venue_id STRING,
                    venue_name STRING,
                    venue_city STRING,
                    venue_country STRING,
                    batting_team_id STRING,
                    batting_team_name STRING,
                    bowling_team_id STRING,
                    bowling_team_name STRING,
                    batsman_id STRING,
                    batsman_name STRING,
                    batsman_dob STRING,
                    batsman_nationality STRING,
                    batting_hand STRING,
                    batsman_role STRING,
                    batsman_email STRING,
                    batsman_phone STRING,
                    batsman_icc_rating STRING,
                    bowler_id STRING,
                    bowler_name STRING,
                    bowler_nationality STRING,
                    bowling_style STRING,
                    bowler_icc_rating STRING,
                    contract_id STRING,
                    contract_grade STRING,
                    annual_retainer_usd STRING,
                    contract_status STRING,
                    drs_review_id STRING,
                    drs_reviewed_by STRING,
                    drs_original_decision STRING,
                    drs_outcome STRING,
                    _FILE_NAME STRING,
                    _LOADED_AT TIMESTAMP_NTZ
                )
                """,
            ],
        )

        # ----------------------------------------------------
        # TASK 2: MIGRATE AN EXISTING TABLE IF NECESSARY
        #
        # CREATE TABLE IF NOT EXISTS does not modify existing
        # tables. This block handles the old column names.
        # ----------------------------------------------------

        migrate_raw_table = SQLExecuteQueryOperator(
            task_id="migrate_raw_table_schema",
            conn_id=CONN_ID,
            sql=[
                """
                DECLARE
                    v_count INTEGER;
                BEGIN
                    SELECT COUNT(*)
                    INTO :v_count
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'RAW'
                      AND TABLE_NAME = 'CRICKET'
                      AND COLUMN_NAME = 'EXTRA_RUN';

                    IF (v_count > 0) THEN
                        ALTER TABLE RAW.CRICKET
                            RENAME COLUMN EXTRA_RUN TO EXTRA_RUNS;
                    END IF;
                END;
                """,
                """
                DECLARE
                    v_count INTEGER;
                BEGIN
                    SELECT COUNT(*)
                    INTO :v_count
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'RAW'
                      AND TABLE_NAME = 'CRICKET'
                      AND COLUMN_NAME = 'TOTAL_RUN';

                    IF (v_count > 0) THEN
                        ALTER TABLE RAW.CRICKET
                            RENAME COLUMN TOTAL_RUN TO TOTAL_RUNS;
                    END IF;
                END;
                """,
                """
                DECLARE
                    v_count INTEGER;
                BEGIN
                    SELECT COUNT(*)
                    INTO :v_count
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'RAW'
                      AND TABLE_NAME = 'CRICKET'
                      AND COLUMN_NAME = 'IS_WICHER';

                    IF (v_count > 0) THEN
                        ALTER TABLE RAW.CRICKET
                            RENAME COLUMN IS_WICHER TO IS_WICKET;
                    END IF;
                END;
                """,
                """
                DECLARE
                    v_count INTEGER;
                BEGIN
                    SELECT COUNT(*)
                    INTO :v_count
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'RAW'
                      AND TABLE_NAME = 'CRICKET'
                      AND COLUMN_NAME = 'DISMISSAL';

                    IF (v_count > 0) THEN
                        ALTER TABLE RAW.CRICKET
                            RENAME COLUMN DISMISSAL TO DISMISSAL_TYPE;
                    END IF;
                END;
                """,
                """
                DECLARE
                    v_count INTEGER;
                BEGIN
                    SELECT COUNT(*)
                    INTO :v_count
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'RAW'
                      AND TABLE_NAME = 'CRICKET'
                      AND COLUMN_NAME = 'BALL_SPEED';

                    IF (v_count > 0) THEN
                        ALTER TABLE RAW.CRICKET
                            RENAME COLUMN BALL_SPEED TO BALL_SPEED_KMPH;
                    END IF;
                END;
                """,
                """
                ALTER TABLE RAW.CRICKET
                    ADD COLUMN IF NOT EXISTS _FILE_NAME STRING
                """,
                """
                ALTER TABLE RAW.CRICKET
                    ADD COLUMN IF NOT EXISTS _LOADED_AT TIMESTAMP_NTZ
                """,
            ],
        )

        # ----------------------------------------------------
        # TASK 3: UPLOAD CSV TO SNOWFLAKE STAGE
        # ----------------------------------------------------

        put_csv = SQLExecuteQueryOperator(
            task_id="put_csv_to_stage",
            conn_id=CONN_ID,
            sql=(
                f"PUT file://{CSV_FILE} "
                "@RAW.INGEST_STAGE/cricket "
                "AUTO_COMPRESS=TRUE "
                "OVERWRITE=TRUE "
                "PARALLEL=8"
            ),
        )

        # ----------------------------------------------------
        # TASK 4: LOAD CSV INTO RAW.CRICKET
        # ----------------------------------------------------

        copy_csv = SQLExecuteQueryOperator(
            task_id="copy_csv_to_raw",
            conn_id=CONN_ID,
            sql="""
                COPY INTO RAW.CRICKET (
                    DELIVERY_ID,
                    DELIVERY_TIMESTAMP,
                    INNING_NO,
                    OVER_NO,
                    BALL_IN_OVER,
                    RUNS_OFF_BAT,
                    EXTRA_TYPE,
                    EXTRA_RUNS,
                    TOTAL_RUNS,
                    IS_WICKET,
                    DISMISSAL_TYPE,
                    BALL_SPEED_KMPH,
                    NON_STRIKER_ID,
                    SHOT_TYPE,
                    SHOT_REGION,
                    MATCH_ID,
                    MATCH_DATE,
                    MATCH_FORMAT,
                    TOURNAMENT_NAME,
                    SEASON,
                    MATCH_STAGE,
                    TOSS_DECISION,
                    VENUE_ID,
                    VENUE_NAME,
                    VENUE_CITY,
                    VENUE_COUNTRY,
                    BATTING_TEAM_ID,
                    BATTING_TEAM_NAME,
                    BOWLING_TEAM_ID,
                    BOWLING_TEAM_NAME,
                    BATSMAN_ID,
                    BATSMAN_NAME,
                    BATSMAN_DOB,
                    BATSMAN_NATIONALITY,
                    BATTING_HAND,
                    BATSMAN_ROLE,
                    BATSMAN_EMAIL,
                    BATSMAN_PHONE,
                    BATSMAN_ICC_RATING,
                    BOWLER_ID,
                    BOWLER_NAME,
                    BOWLER_NATIONALITY,
                    BOWLING_STYLE,
                    BOWLER_ICC_RATING,
                    CONTRACT_ID,
                    CONTRACT_GRADE,
                    ANNUAL_RETAINER_USD,
                    CONTRACT_STATUS,
                    DRS_REVIEW_ID,
                    DRS_REVIEWED_BY,
                    DRS_ORIGINAL_DECISION,
                    DRS_OUTCOME,
                    _FILE_NAME,
                    _LOADED_AT
                )
                FROM (
                    SELECT
                        t.$1,
                        t.$2,
                        t.$3,
                        t.$4,
                        t.$5,
                        t.$6,
                        t.$7,
                        t.$8,
                        t.$9,
                        t.$10,
                        t.$11,
                        t.$12,
                        t.$13,
                        t.$14,
                        t.$15,
                        t.$16,
                        t.$17,
                        t.$18,
                        t.$19,
                        t.$20,
                        t.$21,
                        t.$22,
                        t.$23,
                        t.$24,
                        t.$25,
                        t.$26,
                        t.$27,
                        t.$28,
                        t.$29,
                        t.$30,
                        t.$31,
                        t.$32,
                        t.$33,
                        t.$34,
                        t.$35,
                        t.$36,
                        t.$37,
                        t.$38,
                        t.$39,
                        t.$40,
                        t.$41,
                        t.$42,
                        t.$43,
                        t.$44,
                        t.$45,
                        t.$46,
                        t.$47,
                        t.$48,
                        t.$49,
                        t.$50,
                        t.$51,
                        t.$52,
                        METADATA$FILENAME,
                        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                    FROM @RAW.INGEST_STAGE/cricket t
                )
                FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
                ON_ERROR = 'CONTINUE'
            """,
        )

        # ----------------------------------------------------
        # TASK 5: VERIFY RAW DATA LOAD
        # ----------------------------------------------------

        verify_raw = SQLExecuteQueryOperator(
            task_id="verify_raw_data",
            conn_id=CONN_ID,
            sql="""
                SELECT
                    COUNT(*) AS TOTAL_ROWS,
                    COUNT(_FILE_NAME) AS ROWS_WITH_FILE_METADATA,
                    COUNT(_LOADED_AT) AS ROWS_WITH_LOAD_TIMESTAMP
                FROM RAW.CRICKET
            """,
        )

        # ----------------------------------------------------
        # TASK DEPENDENCIES
        # ----------------------------------------------------

        init_snowflake >> migrate_raw_table
        migrate_raw_table >> put_csv
        put_csv >> copy_csv
        copy_csv >> verify_raw

    with TaskGroup(group_id="dbt_medallion_layers") as dbt_medallion_layers:
        dbt_bronze = BashOperator(
            task_id="dbt_build_bronze",
            bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --select bronze",
        )

        dbt_silver = BashOperator(
            task_id="dbt_build_silver",
            bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --select silver",
        )

        dbt_gold = BashOperator(
            task_id="dbt_build_gold",
            bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --select gold",
        )
        dbt_bronze >> dbt_silver >> dbt_gold
    # ----------------------------------------------------
    # DAG Task Dependencies
    # ----------------------------------------------------
    raw_ingestion >> dbt_medallion_layers