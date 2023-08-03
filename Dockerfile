FROM quay.io/astronomer/astro-runtime:8.8.0

# install dbt into a virtual environment
# replace dbt-postgres with the adapter you need
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate

ENV AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES = airflow\.* astro\.*