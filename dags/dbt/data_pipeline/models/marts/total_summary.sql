WITH date_summary AS (
    SELECT
        MIN(transaction_time) AS min_date,
        MAX(transaction_time) AS max_date
    FROM {{ ref('total_transactions') }}
),

summary AS (
    SELECT
        category,
        sum(amount_pound) as total_spend,
        {{percent_income_spend('total_spend')}} as percent_income
    FROM
        {{ ref('total_transactions')}}
    WHERE 
        category != 'INCOME'
    GROUP BY
        category
)

SELECT 
    summary.*,
    CONCAT('From ',
    TO_VARCHAR(date_summary.min_date::TIMESTAMP,'YYYY-MM-DD'),
    ' to ',
    TO_VARCHAR(date_summary.max_date::TIMESTAMP,'YYYY-MM-DD')) AS data_range
FROM summary, date_summary
ORDER BY total_spend DESC
