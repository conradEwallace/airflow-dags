select 
    feedID,
    currency,
    amount as amount_penny,
    {{ penny_to_pounds ('amount_penny') }} as amount_pound,
    spending_category as category,
    counter_party as counter_party,
    direction,
    source,
    source_method,
    reference,
    transaction_time,
    ingestion_time
from
    {{ ref('stg_stlg_transactions')}} as total_transactions
order by
    transaction_time desc