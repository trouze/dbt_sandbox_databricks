select
    order_id,
    customer_id,
    date(order_date) as order_date,
    order_status,
    current_timestamp() as last_model_run
from {{ ref('stg_orders') }}