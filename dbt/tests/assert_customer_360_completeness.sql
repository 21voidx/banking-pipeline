-- Custom test: Every customer in mart_customer_360 must exist in stg_customers

select m.customer_id
from {{ ref('mart_customer_360') }} m
left join {{ ref('stg_customers') }} s on m.customer_id = s.customer_id
where s.customer_id is null
