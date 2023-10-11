"""

## Run ELT on energy capacity data with Cosmos and dbt Core

Shows how to use the Cosmos, to create an Airflow task group from a dbt project.
The data is loaded into a database and analyzed using the Astro Python SDK. 
"""

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from astro import sql as aql
from astro.sql.table import Table, Metadata
from astro.files import File
from pendulum import datetime
import pandas as pd
import logging
import os

task_logger = logging.getLogger("airflow.task")

CSV_FILEPATH = "include/subset_energy_capacity.csv"
CONNECTION_ID = "db_conn"
DB_NAME = "postgres"
SCHEMA_NAME = "postgres"
# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/my_energy_project"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@aql.dataframe
def log_data_analysis(df: pd.DataFrame):
    """Analyzes the energy capacity information from the input table in order
    to log a table of % Solar and % renewable energy capacity per year.
    If the latest year in the data was also the year with the highest % of solar
    capacity and/or the year with the highest % of renewables capacity a
    celebratory message is logged as well."""

    latest_year = df.YEAR.max()
    year_with_the_highest_solar_pct = df.loc[df["SOLAR_PCT"].idxmax(), "YEAR"]
    year_with_the_highest_renewables_pct = df.loc[df["RENEWABLES_PCT"].idxmax(), "YEAR"]
    df["% Solar"] = round(df["SOLAR_PCT"] * 100, 2)
    df["% Renewable Energy Sources"] = round(df["RENEWABLES_PCT"] * 100, 2)

    task_logger.info(
        df[["YEAR", "% Solar", "% Renewable Energy Sources"]]
        .sort_values(by="YEAR", ascending=True)
        .drop_duplicates()
    )

    if latest_year == year_with_the_highest_solar_pct:
        task_logger.info(
            f"Yay! In {df.COUNTRY.unique()[0]} adoption of solar energy is growing!"
        )
    if latest_year == year_with_the_highest_renewables_pct:
        task_logger.info(
            f"Yay! In {df.COUNTRY.unique()[0]} adoption of renewable energy is growing!"
        )


@dag(
    start_date=datetime(2023, 3, 26),
    schedule=None,
    catchup=False,
)
def my_energy_dag():
    # Astro SDK task that loads information from the local csv into a relational database
    # the table named 'energy' will be created by this task if it does not exist yet
    load_data = aql.load_file(
        input_file=File(CSV_FILEPATH),
        output_table=Table(
            name="energy",
            conn_id=CONNECTION_ID,
            metadata=Metadata(
                database=DB_NAME,
                schema=SCHEMA_NAME,
            ),
        ),
    )

    # use the DbtTaskGroup class to create a task group containing task created
    # from dbt models
    dbt_tg = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"country_code": "CH"}',
        },
        default_args={"retries": 2},
    )

    (
        load_data
        >> dbt_tg
        >> log_data_analysis(
            Table(
                name="create_pct",
                metadata=Metadata(
                    database=DB_NAME,
                    schema=SCHEMA_NAME,
                ),
                conn_id=CONNECTION_ID,
            )
        )
    )


my_energy_dag()
