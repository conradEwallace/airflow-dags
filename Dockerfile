FROM quay.io/astronomer/astro-runtime:12.2.0

# Install git

USER root 

RUN apt-get update && \
    apt-get install -y git && \ 
    apt-get clean

USER astro

# Set up dbt virtual environment
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate