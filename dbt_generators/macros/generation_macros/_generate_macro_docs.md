{% docs generate_commands %}

Generates commands to run for a given source system to generate all necessary files.

Outputs to log.

Running in dbt cloud:

```dbt run-operation generate_commands --args '{"source_name": "salesforce"}'```

{% enddocs %}

{% docs macro_generate_consumption_models %}

Generates intermediate and publish models. Outputs to logs as sql.

Running in dbt cloud:

```dbt run-operation generate_consumption_models --args '{"from_schema_name": "salesforce", "file_name_prefix": "salesforce_int__", "type": "int"}'```

Save log output to your machine as log.sql. Change the "09:04" to match the timestamp from the logs. (It might be easier to copy&paste the log, downloaded log has some interesting whitespace in it)

For splitting the file:

```grep -vwE "09:04" ../../../log.sql | awk '/.sql/{out=$1;next} {print > out;}'```

{% enddocs %}

{% docs macro_generate_multiple_model_yaml %}

Generates yaml definitions for multiple models. Outputs to logs as yml.

Running in dbt cloud: 

```dbt run-operation generate_multiple_model_yaml --args '{"table_prefix": "sap_stg__"}'```

Running with Primary Key tests:

```dbt run-operation generate_multiple_model_yaml --args '{"table_prefix": "sap_int__", "add_pk_test": True, "source_table_prefix": "sap_ext", "source_name": "sap"}'```

Copy&paste output to correct place.

{% enddocs %}

{% docs macro_generate_snapshot_sql %}

Generates snapshot sql for multiple models. Outputs to logs as sql.

Running in dbt cloud:

```dbt run-operation generate_snapshot_sql --args '{"source_name": "sap"}'```

Save log output to your machine. Change the "09:04" to match the timestamp from the logs. (It might be easier to copy&paste the log, downloaded log has some interesting whitespace in it)

For splitting the file:

```grep -vwE "09:04" ../../../log.sql | awk '/.sql/{out=$1;next} {print > out;}'```

{% enddocs %}

{% docs macro_generate_snowflake_staging_sql %}

Generates staging sql for multiple models. Outputs to logs as sql.

Running in dbt cloud:

```dbt run-operation generate_snowflake_staging_sql --args '{"source_name": "sap"}'```

Save log output to your machine. Change the "09:04" to match the timestamp from the logs. (It might be easier to copy&paste the log, downloaded log has some interesting whitespace in it)

For splitting the file:

```grep -vwE "09:04" ../../../log.sql | awk '/.sql/{out=$1;next} {print > out;}'```

{% enddocs %}

{% docs macro_generate_source_yaml_from_meta_table %}

Generates source definition from meta control table from seeds.

Running in dbt cloud:

```dbt run-operation generate_source_yaml_from_meta_table --args '{"meta_file": "meta_sap_control_table", "source_system": "sap"}'```

Copy&paste results to ```models/system/staging/_system__sources.yml```

{% enddocs %}
