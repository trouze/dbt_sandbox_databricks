-- macros/persist_docs_op.sql
{% macro persist_docs_op(relation = true, columns = false) %}

  {% if execute %}
    {% for model_node in graph.nodes.values() %}

      {% set relation = adapter.get_relation(
        database=model_node.database, schema=model_node.schema, identifier=model_node.alias
      ) %}
      
      {% if relation %}
        {% if model_node.description != '' %}
            {{ log("Altering relation comment for " + model_node.alias, info = true) }}
            {{ run_query(alter_table_comment(relation, model_node)) }}
        {% endif %}
      {% endif %}

      {% if columns %}
        {{ log("Altering column comments for " + model_node.alias, info = true) }}
        {{ run_query(alter_column_comment(relation, model_node.columns)) }}
      {% endif %}
    
    {% endfor %}
  
  {% endif %}

{% endmacro %}

{% macro databricks__alter_column_comment(relation, column_dict) %}
  {% if config.get('file_format', default='delta') in ['delta', 'hudi'] %}
    {% for column_name in column_dict %}
        {% if column_dict[column_name]['description']!='' %}
            {% set comment = column_dict[column_name]['description'] %}
            {% set escaped_comment = comment | replace('\'', '\\\'') %}
            {% set comment_query %}
                alter table {{ relation }} change column
                    {{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }}
                    comment '{{ escaped_comment }}';
            {% endset %}
            {% do run_query(comment_query) %}
        {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}