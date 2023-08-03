## ELT pipeline analyzing renewable energy data with Airflow, dbt Core, Cosmos, and the Astro Python SDK

Welcome! This pipeline uses a DAG to load data about changes in solar and renewable energy capacity in different European countries from a local CSV file into a data warehouse. Transformation steps in dbt Core filter the data for a country selected by the user and calculate the percentage of solar and renewable energy capacity for that country in different years. Depending on the trajectory of the percentage of solar and renewable energy capacity in the selected country, the DAG will print different messages to the logs.

> The pipeline in this repo is described in more detail [in Astronomer's docs](https://docs.astronomer.io/learn/use-case-airflow-dbt).

Key tools used:

- [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html).
- [dbt Core](https://docs.getdbt.com/docs/introduction).
- [Cosmos](https://github.com/astronomer/astronomer-cosmos).
- [Astro Python SDK](https://astro-sdk-python.readthedocs.io/en/stable/index.html).

### Prerequisites

- The [Astro CLI](https://docs.astronomer.io/astro/cli/overview).
- [Docker Desktop](https://www.docker.com/products/docker-desktop).
- Access to a data warehouse supported by dbt Core and the Astro Python SDK. See [dbt supported warehouses](https://docs.getdbt.com/docs/supported-data-platforms) and [Astro Python SDK supported warehouses](https://astro-sdk-python.readthedocs.io/en/stable/supported_databases.html). This example uses a local [PostgreSQL](https://www.postgresql.org/) database with a database called `postgres` and a schema called `postgres`.

### How to run this project

1. Make sure you have the Astro CLI installed and that Docker Desktop is running.
2. Clone this repo.
3. Copy the `.env_example` file to `.env` and replace the connection details with your own.
4. Run `astro dev start` in the repository directory to start Airflow locally.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

5. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with `admin` for both your Username and Password.

6. Run the DAG.

### See also

- Tutorial: [Orchestrate dbt Core jobs with Airflow and Cosmos](https://docs.astronomer.io/learn/airflow-dbt).
- Documentation: [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/).
- Webinar: [Introducing Cosmos: The Easy Way to Run dbt Models in Airflow](https://www.astronomer.io/events/webinars/introducing-cosmos-the-east-way-to-run-dbt-models-in-airflow/).
