{% macro insert_eom(table_name) -%}

    {% set before_delete_count_query %}
    select count(*) from {{ source('pc_attunity_db',table_name) }}
    {% endset %}

    {% set before_delete_count_results = run_query(before_delete_count_query) %}

    {% if execute %}
    {% set before_delete_count = before_delete_count_results.columns[0].values() %}
    {% else %}
    {% set before_delete_count = [0] %}
    {% endif %}

    insert into RAW.UTIL.EOM_CORE_AUDIT (LOAD_DT, TABLE_NAME, EFFECTIVE_DT, AUDIT_NUM, AUDIT_STR) 
    select 
        CONVERT_TIMEZONE('America/Chicago',CURRENT_TIMESTAMP()) as LOAD_DT, 
        '{{ table_name }}' as TABLE_NAME, 
        MAX(UPDATED_ON) as EFFECTIVE_DT, 
        {{ before_delete_count[0] }} as AUDIT_NUM, 
        'Rows in {{ table_name }} before delete' as AUDIT_STR 
    from {{ source('pc_attunity_db',table_name) }};


    DELETE FROM {{ source('pc_attunity_db',table_name) }} t1 USING {{ this.database }}.{{ this.schema }}.{{ this.identifier }} t2 WHERE t1.updated_on = t2.updated_on;


    {% set after_delete_count_query %}
    select count(*) from {{ source('pc_attunity_db',table_name) }}
    {% endset %}

    {% set after_delete_count_results = run_query(after_delete_count_query) %}

    {% if execute %}
    {% set after_delete_count = after_delete_count_results.columns[0].values() %}
    {% else %}
    {% set after_delete_count = [0] %}
    {% endif %}

    insert into RAW.UTIL.EOM_CORE_AUDIT (LOAD_DT, TABLE_NAME, EFFECTIVE_DT, AUDIT_NUM, AUDIT_STR)
    select
        CONVERT_TIMEZONE('America/Chicago',CURRENT_TIMESTAMP()) as LOAD_DT,
        '{{ table_name }}' as TABLE_NAME,
        MAX(UPDATED_ON) as EFFECTIVE_DT,
        {{ after_delete_count[0] }} as AUDIT_NUM,
        'Rows in {{ table_name }} after delete' as AUDIT_STR 
    from {{ source('pc_attunity_db',table_name) }}; 


    {% set source_relation = adapter.get_relation(database='pc_attunity_db', schema='eom_core', identifier=table_name) %}
    
    insert into {{ this.database }}.{{ this.schema }}.{{ this.identifier }} (
        {%- for col in adapter.get_columns_in_relation(source_relation) %}
            {{ col.name }}{% if not loop.last %},{% endif -%}
        {% endfor %}
    )
    select {% for col in adapter.get_columns_in_relation(source_relation) %}{{ col.name }}{% if not loop.last %},{% endif -%}{% endfor %}
    from {{ source('pc_attunity_db',table_name) }};


    {% set after_insert_count_query %}
    select count(*) from {{ source('pc_attunity_db',table_name) }}
    {% endset %}

    {% set after_insert_count_results = run_query(after_insert_count_query) %}

    {% if execute %}
    {% set after_insert_count = after_insert_count_results.columns[0].values() %}
    {% else %}
    {% set after_insert_count = [0] %}
    {% endif %}

    insert into RAW.UTIL.EOM_CORE_AUDIT (LOAD_DT, TABLE_NAME, EFFECTIVE_DT, AUDIT_NUM, AUDIT_STR) 
    select
        CONVERT_TIMEZONE('America/Chicago',CURRENT_TIMESTAMP()) as LOAD_DT,
        '{{ table_name }}' as TABLE_NAME,
        MAX(UPDATED_ON) as EFFECTIVE_DT,
        {{ after_insert_count[0] }} as AUDIT_NUM,
        'Rows in {{ table_name }} after insert' as AUDIT_STR
    from {{ source('pc_attunity_db',table_name) }}; 

{%- endmacro %}