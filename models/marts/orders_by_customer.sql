with c as (
    select * from {{ ref('dim_customers') }}
),
o as (
    select * from {{ ref('fct_orders') }}
),
final as (
    select
        c.customer_id as customer_id,
        c.first_name as first_name,
        c.last_initial as last_initial,
        max(o.order_date) as max_order_date,
        min(o.order_date) as min_order_date,
        count(o.order_date) as num_orders,
        current_timestamp() as last_model_run
    from c
    left join o on o.customer_id = c.customer_id
    where o.customer_id is not null
    group by c.customer_id, c.first_name, c.last_initial
)
select * from final
{% if target.name == 'ci' %}
    limit 10
{% endif %}
