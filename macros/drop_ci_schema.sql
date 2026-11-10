{% macro drop_ci_schema(schema_name) %}
  {#- Drops a pull request's throwaway CI schemas (e.g. CI_PR_4_SILVER, CI_PR_4_GOLD).
      Refuses anything that isn't a CI_PR_ schema. -#}
  {% set prefix = schema_name | upper %}
  {% if not prefix.startswith('CI_PR_') %}
    {{ exceptions.raise_compiler_error("Refusing to drop '" ~ schema_name ~ "': only CI_PR_ schemas can be dropped.") }}
  {% endif %}
  {% set find_schemas %}
    select schema_name
    from {{ target.database }}.information_schema.schemata
    where regexp_like(schema_name, '^{{ prefix }}(_[A-Z]+)?$')
  {% endset %}
  {% set results = run_query(find_schemas) %}
  {% for row in results.rows %}
    {% do run_query("drop schema if exists " ~ target.database ~ "." ~ row[0]) %}
    {{ log("Dropped " ~ target.database ~ "." ~ row[0], info=True) }}
  {% endfor %}
  {% if results.rows | length == 0 %}
    {{ log("No schemas found for " ~ prefix, info=True) }}
  {% endif %}
{% endmacro %}
