{% macro generate_snowflake_staging_sql(source_name=None, table_names=None) %}

{% if execute %}

    {% set sources_to_stage = [] %}

    {% set source_nodes = graph.sources.values() if graph.sources else [] %}

    {% for node in source_nodes %}
        {%- if node.source_name == source_name -%}
            {% if node.external %}
                {% if select %}
                    {% for src in select.split(' ') %}
                        {% if '.' in src %}
                            {% set src_s = src.split('.') %}
                            {% if src_s[0] == node.source_name and src_s[1] == node.name %}
                                {% do sources_to_stage.append(node) %}
                            {% endif %}
                        {% else %}
                            {% if src == node.source_name %}
                                {% do sources_to_stage.append(node) %}
                            {% endif %}
                        {% endif %}
                    {% endfor %}
                {% else %}
                    {% do sources_to_stage.append(node) %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% if sources_to_stage|length == 0 %}
        {% do log('No external sources selected', info = true) %}
    {% endif %}

    {% set joined = []%}

    {% for node in sources_to_stage %}
        {% set table_name = node.name | lower %}
        {%- set source_relation = source(node.source_name, table_name) -%}

        {% set table_name_for_file = table_name.split('__')[1] %}
        {% set file_name = node.source_name + '_stg' + '__' + table_name_for_file %}
        
        {% set file_format = node.external.file_format %}    
        {% set location = node.external.location %}

        {% set copy_into_sql %}
            {{ file_name }}.sql
            {% raw -%}
            {{ 
                config(
                    materialized='copy_into',
                    file_format='{%- endraw -%}{{file_format}}{% raw %}',
                    pattern=None
                    ) 
            }}
            {%- endraw %}
            select
            {%- for item in node.columns.values() %}
            {%- set data_type = item.data_type %}
            {%- set colum_name = item.name | lower %}
                ${{loop.index}}::{{ data_type}} as {{ colum_name }},
            {%- endfor %}
                metadata$filename as source_file_name,
                metadata$file_row_number as source_file_row_number,
                {{ '{{ dbt.current_timestamp_backcompat() }}' }} as load_ts
            from {{location}}
        {% endset %}
        {{ log(copy_into_sql, info=True) }}
        {% do joined.append(copy_into_sql) %}
    {% endfor %}



    {% if execute %}

        {% set joined = copy_into_sql | join ('SPLIT_ME \n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}

    {% endif %}

{% endif %}

{% endmacro %}
