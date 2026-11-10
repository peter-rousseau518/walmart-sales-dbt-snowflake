{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        STAGING / PROD : SILVER, GOLD                     (shared, official schemas)
        DEV / CI       : <target schema>_SILVER, _GOLD    (e.g. DBT_PETER_GOLD, CI_PR_4_GOLD)
        so developers and pull requests never overwrite each other or production.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name in ['staging', 'prod'] -%}
        {{ custom_schema_name | trim | upper }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
