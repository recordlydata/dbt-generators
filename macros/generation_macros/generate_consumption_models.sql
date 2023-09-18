{% macro generate_consumption_models(from_schema_name, table_name=None, table_prefix=None, file_name_prefix='', type='int') %}

{% if execute %}

    {% if table_name %}

        {%- set source_relation = ref(table_name) -%}

        {%- set columns = adapter.get_columns_in_relation(source_relation) -%}
        {% set column_names=columns | map(attribute='name') %}
        {% set base_model_sql %}
        with source as (

            select * from {% raw %}{{ ref({% endraw %}'{{ table_name }}'{% raw %}) }}{% endraw %}
            where dbt_valid_to is null

        ),

        renamed as (

            select
                {%- for column in column_names %}
                {% if not case_sensitive_cols %}{{ column | lower }}{% elif target.type == "bigquery" %}{{ column }}{% else %}{{ "\"" ~ column ~ "\"" }}{% endif %}{{"," if not loop.last}}
                {%- endfor -%}

            from source

        )

        select * from renamed
        {% endset %}

        {% if execute %}

        {{ log(base_model_sql, info=True) }}
        {% do return(base_model_sql) %}

        {% endif %}

    {% else %}

    {% set table_prefix = '' if not table_prefix %}

    {#% set relations = dbt_utils.get_relations_by_pattern(from_schema_name, table_prefix + '%') %#}

    {% set relations = [] %}
    {% set staging_nodes = graph.nodes.values() if graph.nodes else [] %}

    {% for node in staging_nodes 
        | selectattr("config.schema", "equalto", from_schema_name) %}
        {% do relations.append(node) %}
    {% endfor %}

    {# If relations is still empty, we might be looking for snapshots #}
    {# Their correct schema is stored a bit differently in graph #}
    {% if relations == [] %}
    {% for node in staging_nodes 
        | selectattr("schema", "equalto", from_schema_name) %}
        {% do relations.append(node) %}
    {% endfor %}
        {% if relations | length > 0 %}
            {% set is_snapshot = True %}
        {% else %}
            {% set is_snapshot = False %}
        {% endif %}
    {% endif %}

    {# If still empty, search the DB #}
    {% if relations == [] %}
        {% set relations = dbt_utils.get_relations_by_pattern(from_schema_name, table_prefix + '%') %}
    {% endif %}

    {% for node in relations %}
            {% set source_table_name = node.name | lower %}

            {% set all_columns = [] %}
            {% if not is_snapshot %}
                {% for column in node.columns.values() %}
                    {% do all_columns.append(column.name) %}
                {% endfor %}
            {% else %}
                {% set all_columns = dbt_utils.get_filtered_columns_in_relation(from=ref(source_table_name)) %}
            {% endif %}
            
            {% set split_name = node.name.split('__')[1] %}
            {% set table_name = split_name | lower %}
            {% set file_name = file_name_prefix + table_name %}

            {% set base_model_sql %}
            {{file_name}}.sql
            {% if type == 'int' %}
            select
                {%- for column in all_columns %}
                {{ column | lower }}{{"," if not loop.last}}
                {%- endfor %}
            from {% raw %}{{ ref({% endraw %}'{{ source_table_name }}'{% raw %}) }}{% endraw %}
            {% elif type == 'publish' %}
            {%raw%}
            {{
                config(
                    alias='{%endraw%}{{table_name}}{%raw%}'
                )
            }}
            {%endraw%}
            select 
                {{ '{{ dbt_utils.star(ref('~ "'" ~ source_table_name ~ "'" ~ '), except=["dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_loaded_at"]) }}' }}
            {{ 'from {{ ref('~ "'" ~ source_table_name ~ "'" ~ ') }} '}}
            where dbt_valid_to is null
            {% endset %}
            {{ log(base_model_sql, info=True) }}

        {% endfor %}

        {% if execute %}

        {{ log(base_model_sql, info=True) }}
        {% do return(base_model_sql) %}

        {% endif %}


    {% endif %}

{% endif %}

{% endmacro %}