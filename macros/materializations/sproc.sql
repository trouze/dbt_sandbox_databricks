{% materialization sproc, adapter='snowflake', supported_languages=['sql'] -%}
  {% set original_query_tag = set_query_tag() %}

  {%- set language = model['language'] -%}
  {%- set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database,
        type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {%- call statement('main', language=language) -%}
    {{ sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}