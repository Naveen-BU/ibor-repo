-- macros/log_dq_results.sql
-- Logs dbt test results to AUDIT.DQ_CHECK_RESULTS after each run.
-- Called automatically via on-run-end hook in dbt_project.yml.

{% macro log_dq_results(results) %}

  {% if execute %}

    {# Create audit table if it does not exist #}
    {% set create_sql %}
      CREATE TABLE IF NOT EXISTS {{ target.database }}.AUDIT.DQ_CHECK_RESULTS (
          check_id        NUMBER AUTOINCREMENT,
          check_name      VARCHAR(200),
          table_name      VARCHAR(200),
          layer           VARCHAR(20),
          status          VARCHAR(10),
          metric_value    NUMBER(18,4),
          threshold       NUMBER(18,4),
          message         VARCHAR(2000),
          executed_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
      );
    {% endset %}
    {% do run_query(create_sql) %}

    {# Iterate over test results and insert each one #}
    {% for res in results if res.node.resource_type == 'test' %}

      {% set test_name  = res.node.name %}

      {# Extract a clean table/model name from the test node #}
      {% if res.node.test_metadata is defined and res.node.test_metadata is not none %}
        {# Generic test (not_null, unique, relationships, etc.) #}
        {# kwargs.model can be: ref('model'), source('src','tbl'),
           or {{ get_where_subquery(ref('model')) }} — strip everything down to model name #}
        {% set raw_model = res.node.test_metadata.kwargs.get('model', '') | string %}
        {% set cleaned = raw_model
            | replace("{{", "") | replace("}}", "")
            | replace("get_where_subquery(", "") | replace("get_where_subquery (", "")
            | replace("ref(", "") | replace("source(", "")
            | replace("'", "") | replace('"', '')
            | replace(")", "") | replace(",", ".")
            | trim %}
        {# If still messy, fall back to the test's depends_on refs #}
        {% if cleaned == '' or '{{' in cleaned or '{%' in cleaned %}
          {% if res.node.refs | length > 0 %}
            {% set first_ref = res.node.refs[0] %}
            {% set cleaned = first_ref[0] if first_ref is iterable else first_ref | string %}
          {% else %}
            {% set cleaned = 'unknown' %}
          {% endif %}
        {% endif %}
        {% set col = res.node.test_metadata.kwargs.get('column_name', '') %}
        {% if col %}
          {% set table_name = cleaned ~ '.' ~ col %}
        {% else %}
          {% set table_name = cleaned %}
        {% endif %}
      {% elif res.node.refs | length > 0 %}
        {# Singular test — refs is a list; each element may be a RefArgs object or a list #}
        {% set first_ref = res.node.refs[0] %}
        {% if first_ref is string %}
          {% set table_name = first_ref %}
        {% elif first_ref is iterable %}
          {% set table_name = first_ref[0] | default(first_ref | string, true) %}
        {% else %}
          {% set table_name = first_ref | string %}
        {% endif %}
      {% elif res.node.sources | length > 0 %}
        {# Source-based test #}
        {% set src = res.node.sources[0] %}
        {% set table_name = src[0] ~ '.' ~ src[1] if src is iterable else src | string %}
      {% else %}
        {% set table_name = res.node.fqn[-1] | default('unknown', true) %}
      {% endif %}

      {# Map dbt status to DQ status #}
      {% if res.status == 'pass' %}
        {% set dq_status = 'PASS' %}
      {% elif res.status == 'warn' %}
        {% set dq_status = 'WARN' %}
      {% else %}
        {% set dq_status = 'FAIL' %}
      {% endif %}

      {# Build a single string combining all context for layer detection:
         tags, fqn path, test name, table_name, and depends_on node names #}
      {% set ctx_parts = res.node.tags | list %}
      {% set ctx_parts = ctx_parts + res.node.fqn %}
      {% set ctx_parts = ctx_parts + [test_name, table_name] %}
      {% for dep in res.node.depends_on.nodes %}
        {% set ctx_parts = ctx_parts + [dep] %}
      {% endfor %}
      {% set layer_ctx = ctx_parts | join(' ') | lower %}

      {# Determine layer from combined context #}
      {% if 'bronze' in layer_ctx or 'staging' in layer_ctx or 'stg_' in layer_ctx or 'raw_' in layer_ctx %}
        {% set layer = 'BRONZE' %}
      {% elif 'silver' in layer_ctx or 'intermediate' in layer_ctx or 'int_' in layer_ctx %}
        {% set layer = 'SILVER' %}
      {% elif 'gold' in layer_ctx or 'marts' in layer_ctx or 'mart_' in layer_ctx %}
        {% set layer = 'GOLD' %}
      {% else %}
        {% set layer = 'CROSS_LAYER' %}
      {% endif %}

      {% set failures = res.node.config.get('severity', 'ERROR') if dq_status == 'FAIL' else '0' %}
      {% set msg = res.message | default('', true) | truncate(2000, true) %}

      {% set insert_sql %}
        INSERT INTO {{ target.database }}.AUDIT.DQ_CHECK_RESULTS
            (check_name, table_name, layer, status, metric_value, threshold, message)
        VALUES (
            '{{ test_name | replace("'", "''") }}',
            '{{ table_name | replace("'", "''") }}',
            '{{ layer }}',
            '{{ dq_status }}',
            {{ res.failures if res.failures is not none else 0 }},
            0,
            '{{ msg | replace("'", "''") }}'
        );
      {% endset %}
      {% do run_query(insert_sql) %}

    {% endfor %}

    {{ log("DQ results logged to AUDIT.DQ_CHECK_RESULTS", info=True) }}

  {% endif %}

{% endmacro %}
