{% macro persist_docs_op(relation = true, columns = true) %}

  {% if execute %}
    {% for model_node in graph.nodes.values()
        |selectattr("resource_type", "in", ["model","source","seed","snapshot","analyses","macro"]) %}

      {% set relation = adapter.get_relation(
        database=model_node.database, schema=model_node.schema, identifier=model_node.alias
      ) %}
      
      {%- set existing_columns = adapter.get_columns_in_relation(relation) -%}
      {%- set columns_to_persist_docs = adapter.get_persist_doc_columns(existing_columns, model_node.columns) -%}

      {% if relation and not relation.is_view %}
        {{ log("Altering relation comment for " + model_node.alias, info = true) }}
        {{ alter_table_comment(relation, model_node) }}
      {% endif %}

      {% if columns and not relation.is_view %}
        {{ log("Altering column comments for " + model_node.alias, info = true) }}
        {{ alter_column_comment(relation, columns_to_persist_docs) }}
      {% endif %}
    
    {% endfor %}
  
  {% endif %}

{% endmacro %}