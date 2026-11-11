{% macro notify_quarantine() %}
  {#- Runs at the end of every dbt run (on-run-end). In PROD only: if rows were
      quarantined for a real problem (not just an older duplicate load), publish a
      summary to the SNS topic through the WALMART_ERRORS_SNS notification integration. -#}
  {% if execute and target.name == 'prod' %}
    {% set summary_sql %}
      select reject_reason, source_table, count(*) as row_count, min(_file_name) as example_file
      from {{ target.database }}.SILVER.QUARANTINE_ROWS
      where reject_reason not like 'duplicate%'
      group by 1, 2
    {% endset %}
    {% set rows = run_query(summary_sql).rows %}
    {% if rows | length > 0 %}
      {% set details = [] %}
      {% for r in rows %}
        {% do details.append({'reason': r[0], 'table': r[1], 'rows': r[2] | int, 'example_file': r[3]}) %}
      {% endfor %}
      {% set message = {
          'type': 'REJECTED_ROWS',
          'environment': 'PROD',
          'quarantine_table': target.database ~ '.SILVER.QUARANTINE_ROWS',
          'details': details
      } | tojson %}
      {% do run_query(
          "call system$send_snowflake_notification("
          ~ "snowflake.notification.application_json('" ~ message | replace("'", "''") ~ "'), "
          ~ "snowflake.notification.integration('WALMART_ERRORS_SNS'))"
      ) %}
      {{ log("Quarantine alert sent: " ~ message, info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
