{% macro generate_commands(source_name) %}

{% if '-' in source_name %}
  {% do log('Dash in source name, changing to underscore...', info=true) %}
  {% set source_name = source_name|replace('-', '_') %}
{% endif %}

{% set shell_commands %}
==================================================================================================
These commands generate the folders and empty place-holder files. Run in local project with git bash.
==================================================================================================
cd $(git rev-parse --show-toplevel) &&
mkdir models/staging/ &&
mkdir models/staging/{{source_name}} &&
touch models/staging/{{source_name}}/_{{source_name}}__sources.yml &&
touch models/staging/{{source_name}}/_{{source_name}}_stg__models.yml &&
mkdir snapshots/{{source_name}}

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
      publish:
        +transient: false
        +schema: {{source_name}}
        +materialized: table
        +post-hook: {%raw%}"{{ grant_usage('ROLE_DATASET_{%endraw%}{{source_name|upper}}{%raw%}_ENV_R', schema=schema) }}"{%endraw%}
        +grants:
          +select: ["ROLE_DATASET_{{source_name|upper}}_{%raw%}{{ env_var('DBT_TARGET_ENV') }}{%endraw%}_R"]

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

Save as:
models/staging/{{source_name}}/_{{source_name}}__sources.yml

==================================================================================================
CREATE DBT SNAPSHOTS aka PERSISTENT STAGING WITH HISTORY
==================================================================================================

Create persistent staging:
dbt run-operation generate_snapshot_sql --args '{"source_name": "{{source_name}}"}'

Save and split generated sql to snapshot/{{source_name}}/
grep -vwP "\d\d:\d\d" ../../log.sql | awk '/.sql/{out=$1;next} {print > out;}'

Run:
dbt snapshot --select {{source_name}}

==================================================================================================
CREATE INTERMEDIATE LAYER
==================================================================================================

Create templates:
dbt run-operation generate_consumption_models --args '{"from_schema_name": "dbt_cloud_{{source_name}}_snapshot", "file_name_prefix": "{{source_name}}_int__", "type": "int"}'

Build only Intermediate models:
dbt build --select {{source_name}}.intermediate

Add docs and Primary Key tests:
dbt run-operation generate_multiple_model_yaml --args '{"table_prefix": "{{source_name}}_int__%", "add_pk_test": True, "source_table_prefix": "{{source_name}}_ext", "source_name": "{{source_name}}", "limit_pk_test": true}'

==================================================================================================
CREATE PUBLISH
==================================================================================================

Create publish:
dbt run-operation generate_consumption_models --args '{"from_schema_name": "{{source_name}}_history", "file_name_prefix": "{{source_name}}_publish__", "type": "publish"}'

Save and split to to models/{{source_name}}/publish/:
grep -vwP "\d\d:\d\d" ../../../log.sql | awk '/.sql/{out=$1;next} {print > out;}'

Build only Publish models:
dbt build --select {{source_name}}.publish

Create docs and PKs:
dbt run-operation generate_multiple_model_yaml --args '{"schema_pattern": "%_{{source_name}}", "add_pk_test": True, "source_table_prefix": "{{source_name}}_ext", "source_name": "{{source_name}}", "alias_prefix" : "{{source_name}}_publish__"}'

==================================================================================================
BUILD FULL MODEL
==================================================================================================

dbt build --select {{source_name}}

{% endset %}

{% do log(shell_commands, info=True) %}


{% endmacro %}