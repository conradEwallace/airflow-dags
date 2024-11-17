import os
import subprocess
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from cosmos.profiles import SnowflakeUserPasswordProfileMapping



with DAG(
    dag_id="starling_ingest",
    default_args = {"owner":"cwallace","start_date":datetime(2024,11,17)},
    schedule_interval = None,
    catchup=False
) as dag: 
    #Task 1 - Run the Starling Ingestion script
    run_starling_ingest_script = BashOperator(
        task_id="starling_ingest",
        bash_command="python /usr/local/airflow/include/scripts/starling_sfk_ingest.py"
    )

    trigger_dbt_dag_task = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="dbt_dag_starling"
    )
    
run_starling_ingest_script >> trigger_dbt_dag_task




