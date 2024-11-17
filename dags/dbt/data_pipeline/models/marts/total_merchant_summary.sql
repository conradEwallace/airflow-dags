WITH date_summary AS (
    SELECT 
        MIN(transaction_time) AS min_date,
        MAX(transaction_time) AS max_date
    FROM {{ ref('total_transactions') }}
),

summary AS (
    SELECT
        counter_party,
        category,
        SUM(amount_pound) AS total_spend,
        {{ percent_income_spend('total_spend') }} AS percent_income
    FROM {{ ref('total_transactions') }}
    WHERE counter_party NOT IN ('BELLO ZOO/2021', 'C Wallace', 'Starling Bank')
    GROUP BY counter_party, category
)

SELECT 
    summary.*,
    CONCAT(
        'From ', 
        TO_VARCHAR(date_summary.min_date::TIMESTAMP, 'YYYY-MM-DD'), 
        ' to ', 
        TO_VARCHAR(date_summary.max_date::TIMESTAMP, 'YYYY-MM-DD')
    ) AS date_range
FROM summary, date_summary
ORDER BY total_spend DESC