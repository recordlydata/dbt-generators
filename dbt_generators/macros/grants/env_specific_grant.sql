{% macro env_specific_grant(target_environment, role, warehouse, grant='select', relation=this) %}

{% if '_ENV_' in role %}
    {% set role = role|replace("_ENV_", '_' ~ env_var('DBT_TARGET_ENV') ~ '_') %}
{% elif '_ENV' in role %}
    {% set role = role|replace("_ENV", '_' ~ env_var('DBT_TARGET_ENV')) %}
{% else %}
    {% set role = role %}
{% endif %}

{% if '_ENV_' in warehouse %}
    {% set warehouse = warehouse|replace("_ENV_", '_' ~ env_var('DBT_TARGET_ENV') ~ '_') %}
{% elif '_ENV' in warehouse %}
    {% set warehouse = warehouse|replace("_ENV", '_' ~ env_var('DBT_TARGET_ENV')) %}
{% else %}
    {% set warehouse = warehouse %}
{% endif %}

{% if execute %}

{% if target_environment == target.name %}

    {% if warehouse %}
        {% call statement('warehouse_sql', fetch_result=True) %}
        grant usage on warehouse {{ warehouse }} to role {{ role }};
        {% endcall %}
        {%- set results = load_result('warehouse_sql') -%}
        {% set result = results['data'][0] %}
        {{log('Granting usage on warehouse ' ~  warehouse ~  ' to role ' ~ role ~ '...', info=False )}}
        {{log(result, info=False)}}
    {% endif %}

    {% if relation %}
        {% call statement('database_sql', fetch_result=True) %}
        grant usage on database {{ this.database }} to role {{ role }};
        {% endcall %}
        {% call statement('schema_sql', fetch_result=True) %}
        grant usage on schema {{ this.schema }} to role {{ role }};
        {% endcall %}

        {%- set results = load_result('database_sql') -%}
        {% set result = results['data'][0] %}
        {{log('Granting usage on database ' ~  database ~  ' to role ' ~ role ~ '...', info=False )}} 
        {{log(result, info=False)}}

        {%- set results = load_result('schema_sql') -%}
        {% set result = results['data'][0] %}
        {{log('Granting usage on schema ' ~  schema ~  ' to role ' ~ role ~ '...', info=False )}}
        {{log(result, info=False)}}

    {% endif %}

    {% if grant %}

        {% call statement('grant_sql', fetch_result=True) %}
        grant {{ grant }} on table {{ this }} to role {{ role }};
        {% endcall %}

        {%- set results = load_result('grant_sql') -%}
        {% set result = results['data'][0] %}
        {{log('Granting ' ~ grant ~ ' on relation ' ~  this ~  ' to role ' ~ role ~ '...', info=False )}} 
        {{log(result, info=False)}}

    {% endif %}

{% else %}

{{log('Macro env_specific_grants: Expected environment: ' ~ target_environment ~ '. Got: ' ~ target.name ~ '. Not granting environment specific grants.', info=False )}} 

{% endif %}

{% endif %}

{% endmacro %}