FROM apache/airflow:2.10.5

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake
RUN pip install --no-cache-dir apache-airflow-providers-snowflake