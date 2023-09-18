{% macro generate_snapshot_sql(source_name, table_names=None) %}

{% if execute %}

    {% if table_names is none %}
    {% set tables = [] %}
    {% for node in graph.sources.values() %}
        {%- if node.source_name == source_name -%}
            {% do tables.append(node.name) %}
        {% endif %}
    {% endfor %}
    {% else %}
    {% set tables = table_names %}
    {% endif %}

    {% set joined = []%}

    {% for table in tables %}
        {% set table_name = table | lower %}

        {% set bare_table_name = table_name.split('__')[1] %}
        {% set file_name = source_name + '_snapshot__' + bare_table_name %}
        {% set table_name_in_staging = source_name + '_stg__' + bare_table_name %}

        {% set primary_keys = [] -%}
        {% set is_delta_list = [] -%}
        {% set load_timestamp_list = [] -%}
        {% for node in graph.sources.values() -%}
            {% set table_name_in_source = source_name + '_ext' + '__' + bare_table_name %}
            {%- if node.source_name == source_name and node.name == table_name_in_source  -%}
                {% for column in node.columns.values()
                    | selectattr("meta.is_primary_key") %}
                    {%- do primary_keys.append(column.name) -%}
                {% endfor %}
                {% for column in node.columns.values()
                    | selectattr("meta.is_load_timestamp") %}
                    {%- do load_timestamp.append(column.name) -%}
                {% endfor %}
                {% do is_delta_list.append(node.meta.is_delta) %}
            {%- endif -%}
        {%- endfor %}
        {%- set is_delta = is_delta_list[0] -%}
        {%- set updated_at = load_timestamp_list[0] -%}
        {%- set updated_at = updated_at if updated_at else 'UPDATE_ME_TO_CORRECT_TIMESTAMP' -%}
        
        {% set config_sql %}
        {%- if is_delta -%}
            {%- raw -%}
            {{
                config(
                unique_key='load_generated_key',
                strategy="check",
                check_cols=["load_generated_key", "row_checksum"],
                batch_type="file",
                batch_source_column="source_file_name",
                )
            }}
            {%- endraw -%}
        {%- else -%}
            {%- raw -%}
            {{
                config(
                unique_key='load_generated_key',
                strategy="check",
                check_cols=["load_generated_key", "row_checksum"],
                invalidate_hard_deletes=True,
                batch_type="file",
                batch_source_column="source_file_name",
                )
            }}
            {%- endraw -%}
        {%- endif -%}
        {%- endset -%}

        {% set snapshot_sql %}
            {{ file_name }}.sql
            {{ '{% snapshot ' ~ file_name ~ ' %}' }}

            {{ config_sql }}

            with
                snp as (
            select 
                {{ '{{ dbt_utils.star(ref('~ "'" ~ table_name_in_staging ~ "'" ~ ')) }},' }}
                {{ '{{ dbt_utils.generate_surrogate_key(' ~ primary_keys ~ ') }} as load_generated_key,' }}
                {{ '{{ dbt_utils.generate_surrogate_key(dbt_utils.get_filtered_columns_in_relation(from=ref('~ "'" ~ table_name_in_staging ~ "'" ~ '), except=['}}
                {{      "'source_file_name',"}}
                {{      "'source_file_row_number',"}}
                {{      "'load_ts',"}}
                {{      "'" ~ updated_at ~ "'"}}
                {{      ']) ) }} as row_checksum' }}
                {{ 'from {{ ref('~ "'" ~ table_name_in_staging ~ "'" ~ ') }} as a'}}
                {{ "where source_file_name = '__BATCH__'"}}
            )
            {%- raw %}

            {{ dbt_utils.deduplicate(
                relation='snp',
                partition_by='load_generated_key',
                order_by="source_file_name desc, source_file_row_number desc",
            )
            }}

            {% endsnapshot %}
            
            {% endraw %}
        {% endset %}
        {{ log(snapshot_sql, info=True) }}
        {% do joined.append(snapshot_sql) %}
    {% endfor %}



    {% if execute %}

        {% set joined = snapshot_sql | join ('SPLIT_ME \n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}

    {% endif %}

{% endif %}

{% endmacro %}
