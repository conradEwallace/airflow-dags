WITH latest_ingestion AS (
    SELECT *
    FROM {{ source('starling_transactions', 'raw_transactions') }}
    WHERE ingestion_time = (SELECT MAX(ingestion_time) FROM {{ source('starling_transactions', 'raw_transactions') }})
),

flattened_data AS (
    SELECT 
        ingestion_time,
        flattened.value AS flattened_json
    FROM latest_ingestion,
    LATERAL FLATTEN(input => latest_ingestion.raw_json) AS flattened
)

SELECT
    flattened_json:"feedItemUid"::STRING AS feedID,
    flattened_json:"amount":"currency"::STRING AS currency,
    flattened_json:"amount":"minorUnits"::NUMBER AS amount,
    flattened_json:"counterPartyName"::STRING AS counter_party,
    flattened_json:"spendingCategory"::STRING AS spending_category,
    flattened_json:"direction"::STRING AS direction,
    flattened_json:"source"::STRING AS source,
    flattened_json:"sourceSubType"::STRING AS source_method,
    flattened_json:"reference"::STRING AS reference,
    flattened_json:"transactionTime"::STRING AS transaction_time,
    ingestion_time
FROM flattened_data
