{% macro drop_ci_schema(schema_name) %}
  {#- Drops a pull request's throwaway CI schema. Refuses anything that isn't a CI schema. -#}
  {% if not schema_name.upper().startswith('CI_PR_') %}
    {{ exceptions.raise_compiler_error("Refusing to drop '" ~ schema_name ~ "': only CI_PR_ schemas can be dropped.") }}
  {% endif %}
  {% set sql = "drop schema if exists " ~ target.database ~ "." ~ schema_name | upper %}
  {% do run_query(sql) %}
  {{ log("Dropped " ~ target.database ~ "." ~ schema_name | upper, info=True) }}
{% endmacro %}
