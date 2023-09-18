{% macro grant_usage(role, warehouse=var('default_reader_warehouse'), database=target.database, schema=none) %}

{% if '_ENV_' in role %}
    {% set role = role|replace("_ENV_", '_' ~ env_var('DBT_TARGET_ENV') ~ '_') %}
{% elif '_ENV' in role %}
    {% set role = role|replace("_ENV", '_' ~ env_var('DBT_TARGET_ENV')) %}
{% else %}
    {% set role = role %}
{% endif %}

{% if execute %}

    {% if warehouse %}
        {% call statement('warehouse_sql', fetch_result=True) %}
        grant usage on warehouse {{ warehouse }} to role {{ role }};
        {% endcall %}
    {% endif %}

    {% if database %}
        {% call statement('database_sql', fetch_result=True) %}
        grant usage on database {{ database }} to role {{ role }};
        {% endcall %}
    {% endif %}

    {% if schema %}
        {% call statement('schema_sql', fetch_result=True) %}
        grant usage on schema {{ schema }} to role {{ role }};
        {% endcall %}
    {% endif %}

    {% if warehouse %}
        {%- set results = load_result('warehouse_sql') -%}
        {% set result = results['data'][0] %}
        {{log('Granting usage on warehouse ' ~  warehouse ~  ' to role ' ~ role ~ '...', info=False )}}
        {{log(result, info=False)}}
    {% endif %}

    {% if database %}
        {%- set results = load_result('database_sql') -%}
        {% set result = results['data'][0] %}
        {{log('Granting usage on database ' ~  database ~  ' to role ' ~ role ~ '...', info=False )}}
        {{log(result, info=False)}}
    {% endif %}

    {% if schema %}
        {%- set results = load_result('schema_sql') -%}
        {% set result = results['data'][0] %}
        {{log('Granting usage on schema ' ~  schema ~  ' to role ' ~ role ~ '...', info=False )}}
        {{log(result, info=False)}}
    {% endif %}

{% endif %}

{% endmacro %}