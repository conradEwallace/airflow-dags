select distinct 
    counter_party,
    category 
from 
    {{ ref('total_transactions')}} 
order by 
    category
