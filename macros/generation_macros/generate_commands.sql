{% macro generate_commands(source_name) %}

{% if '-' in source_name %}
  {% do log('Dash in source name, changing to underscore...', info=true) %}
  {% set source_name = source_name|replace('-', '_') %}
{% endif %}

{% set shell_commands %}
==================================================================================================
COPY TO dbt_project.yml MODELS AND SNAPSHOTS
==================================================================================================

MODELS
  ------THIS ENTRY PROBABLY EXISTS-------
    staging:
  ------COPY FROM HERE-------
      staging:
        +schema: {{source_name}}_stg
        +materialized: copy_into

SNAPSHOTS:
    # ------------------
    # {{source_name}}
    # ------------------
    {{source_name}}:
      +target_schema: dbt_tpch_{{source_name}}_snapshot

==================================================================================================
CREATE SOURCE YML DESCRIBING TABLES FOR DBT
==================================================================================================

Run:
dbt run-operation generate_source --args '{"schema_name": "SOURCE_SCHEMA_NAME", "database_name": "SOURCE_DATABASE_NAME", "generate_columns": "true", "include_descriptions": "true", "include_data_types": "true", "name": "{{source_name}}", "include_database": "true", "include_schema": "true"}' > _{{source_name}}__sources.yml &&
mv _{{source_name}}__sources.yml models/staging/{{source_name}}/_{{source_name}}__sources.yml

==================================================================================================
CREATE DBT SNAPSHOTS aka PERSISTENT STAGING WITH HISTORY
==================================================================================================

Create persistent staging:
dbt run-operation generate_snapshots --args '{"source_name": "{{source_name}}"}'

Run:
dbt snapshot --select {{source_name}}

==================================================================================================
CREATE STAGING LAYER
==================================================================================================

dbt run-operation generate_stg_models --args '{"source_name": "{{source_name}}"}'   

Create yaml:

dbt run-operation generate_model_yaml

==================================================================================================
CREATE INTERMEDIATE
==================================================================================================

The place for complex transformations.

If no complex transformations, but a publish model is needed,

create a "select * from staging layer"

so we can add tests on a good place.

Create yml for tests:

==================================================================================================
CREATE PUBLISH
==================================================================================================

Create publish:

"select * from intermediate layer"

Create yml for tests and docs:

==================================================================================================
BUILD FULL MODEL
==================================================================================================

dbt build --select {{source_name}}

{% endset %}

{% do log(shell_commands, info=True) %}


{% endmacro %}