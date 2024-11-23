#API - Snowflake Ingest

# Imports 
import requests
import sys
from datetime import datetime, date
import os
import io
import snowflake.connector
import json
from airflow.models import Variable
from airflow.hooks.base import BaseHook


def fetch_and_save_connection_details():
    conn = BaseHook.get_connection("starling_raw_snowflake_conn")
    conn_details = {
          "user": conn.login,
          "password": conn.password,
          "account": conn.extra_dejson.get("account"),
          "warehouse": "dbt_wh",
          "database":"starling_db",
          "schema":"raw_source",
          "role":"dbt_role"
     }
    return conn_details


def get_starling_transactions():
    personal_token_RO = Variable.get('starling_personal_token_RO')
    accountUid = Variable.get('starling_accountUid')
    headers = {'accept':'application/json','Authorization':f'Bearer {personal_token_RO}'}
    start_date='2024-10-01'
    end_date=date.today()

    url = f'https://api.starlingbank.com/api/v2/feed/account/{accountUid}/settled-transactions-between?minTransactionTimestamp={start_date}T00%3A00%3A00.000Z&maxTransactionTimestamp={end_date}T00%3A00%3A00.000Z'

    response = requests.get(url,headers=headers)

    if response.status_code == 200:
            data = response.json()
            return data
    else:
          print(f"Error: {response.status_code}")

def save_transactions_to_file(data, file_path):
    with open(file_path,'w') as json_file:
        json.dump(data.get("feedItems",[]), json_file)

def load_data_to_snowflake(file_path):

    conn_details = fetch_and_save_connection_details()

    conn = snowflake.connector.connect(
        user=conn_details["user"],
        password=conn_details["password"],
        account=conn_details["account"],
        warehouse=conn_details["warehouse"],
        database=conn_details["database"],
        schema=conn_details["schema"],
        role=conn_details["role"]
    )

    try:
        with conn.cursor() as cursor: 
            #Creating a temporary staging table
            cursor.execute("""CREATE OR REPLACE TABLE transaction_staging (raw_json VARIANT)""")

            #PUT the file into the snowflake stage
            cursor.execute(f"CREATE OR REPLACE STAGE temp_stage")
            cursor.execute(f"PUT file://{file_path} @temp_stage")

            #COPY INTO the snowflake staging table from the stage
            copy_query = f"""
            COPY INTO transaction_staging (raw_json)
            FROM @temp_stage
            FILE_FORMAT = ( TYPE = 'JSON')
            ON_ERROR = 'CONTINUE'
            """

            cursor.execute(copy_query)

            #Insert into the snowflake table

            insert_query = f"""
            INSERT INTO raw_transactions (RAW_JSON, INGESTION_TIME)
            SELECT raw_json, CURRENT_TIMESTAMP()
            FROM transaction_staging;
            """

            cursor.execute(insert_query)

            # DROP the staging tables
            cursor.execute(f"DROP STAGE temp_stage")
            cursor.execute(f"DROP TABLE transaction_staging")

    finally:
        # Commit and close the connection
        conn.commit()
        conn.close


#Main Function 
def main():
    #Retrieving the data 
    print("Retrieving Data From Starling...")
    transaction_data = get_starling_transactions()

    #Saving transactions to a local file
    temp_file_path = "transactions.json"
    save_transactions_to_file(transaction_data,temp_file_path)

    print('Loading Data to Snowflake...')
    #Load Data To Snowflake 
    load_data_to_snowflake(temp_file_path)

    #Remove the temporary file after successful load
    if os.path.exists(temp_file_path):
         os.remove(temp_file_path)

    print("Data successfully landed to Snowflake")



if __name__ == "__main__":
    main()
