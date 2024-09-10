{% macro persist_docs_op(relation = true, columns = true) %}

  {% if execute %}
    {% for model_node in graph.nodes.values() %}

      {% set relation = adapter.get_relation(
        database=model_node.database, schema=model_node.schema, identifier=model_node.alias
      ) %}
      
      {% if relation %}
        {{ log("Altering relation comment for " + model_node.alias, info = true) }}
        {{ alter_table_comment(relation, model_node) }}
      {% endif %}

      {% if columns %}
        {{ log("Altering column comments for " + model_node.alias, info = true) }}
        {{ alter_column_comment(relation, model_node.columns) }}
      {% endif %}
    
    {% endfor %}
  
  {% endif %}

{% endmacro %}