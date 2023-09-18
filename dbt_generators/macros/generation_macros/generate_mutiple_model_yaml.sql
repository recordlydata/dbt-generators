{% macro generate_multiple_model_yaml(table_prefix=None, schema_pattern=None, add_pk_test=False, source_table_prefix=None, source_name=None, limit_pk_test=False, alias_prefix=None) %}

{% if execute %}

    {% set schema_pattern = schema_pattern if schema_pattern else '%' %}
    {% set table_prefix = table_prefix if table_prefix else '%' %}

    {% set relations = dbt_utils.get_relations_by_pattern(schema_pattern, table_prefix) %}
    {% set table_list= relations | map(attribute='identifier') %}
    {% set relations_from_db = True %}

    {% if not relations %}
        {% set relations_from_db = False %}
        {% do log('Tables not in database, searching dbt graph...', info = true) %}


        {% set sources_to_stage = [] %}

        {% for node in graph.nodes.values() 
            | selectattr("resource_type", "equalto", "model") %}
            {% if node.name.startswith(table_prefix) %}
                {% do sources_to_stage.append(node) %}
            {% endif %}
        {% endfor %}
        {% if sources_to_stage|length == 0 %}
            {% do log('No external sources selected', info = true) %}
        {% endif %}
    {% endif %}

    {# We have tables in the specified schema, ie. they have been deployed and we have a complete graph #}
    {# Using modified https://github.com/dbt-labs/dbt-codegen/blob/main/macros/generate_source.sql #}

    {% set final_yaml=[] %}
    {% do final_yaml.append('version: 2') %}
    {% do final_yaml.append('') %}
    {% do final_yaml.append('models:') %}

    {% set tables = relations if relations_from_db else sources_to_stage %}

    {% for table in tables %}
        {% set table_name = table.identifier if relations_from_db else table.node.name %}
        {% set table_name = alias_prefix ~ table_name if alias_prefix else table_name %}
        {% set model_yaml = [] %}
        {% do model_yaml.append('      - name: ' ~ table_name | lower ) %}
        {% do model_yaml.append('        description: ""' ) %}
        {% do model_yaml.append('        columns:') %}

            {% if relations_from_db %}
                {% set columns=adapter.get_columns_in_relation(table) if relations_from_db %}
            {% elif not relations_from_db %}
                {%- for item in table.columns.values() %}
                    {% set columns = [] %}
                    {%- do columns.append(item.name | lower) %}
                {% endfor %}
            {% endif %}


            {% for column in columns %}
                {% do model_yaml.append('          - name: ' ~ column.name | lower ) %}
                {%- set description -%}
                    {%- if column.name | lower == 'source_file_name' -%}
                    {%raw%} '{{ doc("source_file_name") }}' {%endraw%}
                    {%- elif column.name | lower == 'source_file_row_number' -%}
                    {%raw%} '{{ doc("source_file_row_number") }}' {%endraw%}
                    {%- elif column.name | lower == 'dbt_loaded_at' -%}
                    {%raw%} '{{ doc("dbt_loaded_at") }}' {%endraw%}
                    {%- elif column.name | lower == 'load_generated_key' -%}
                    {%raw%} '{{ doc("load_generated_key") }}' {%endraw%}
                    {%- elif column.name | lower == 'row_checksum' -%}
                    {%raw%} '{{ doc("row_checksum") }}' {%endraw%}
                    {%- elif column.name | lower == 'dbt_scd_id' -%}
                    {%raw%} '{{ doc("dbt_scd_id") }}' {%endraw%}
                    {%- elif column.name | lower == 'dbt_updated_at' -%}
                    {%raw%} '{{ doc("dbt_updated_at") }}' {%endraw%}
                    {%- elif column.name | lower == 'dbt_valid_from' -%}
                    {%raw%} '{{ doc("dbt_valid_from") }}' {%endraw%}
                    {%- elif column.name | lower == 'dbt_valid_to' -%}
                    {%raw%} '{{ doc("dbt_valid_to") }}' {%endraw%}
                    {%- else -%}
                    ""
                    {%- endif -%}
                    {%- endset -%}
                {% do model_yaml.append('            description: ' ~ description ) %}
            {% endfor %}
            {% if add_pk_test %}
                {% do model_yaml.append('        tests: ') %}
                {% do model_yaml.append('          - dbt_constraints.primary_key:') %}
                
                {% set primary_keys = [] -%}
                {% set whole_table_name = table.identifier.split('__') %}
                {% if whole_table_name | length > 1 %}
                    {% set split_table_name = whole_table_name[1] %}
                {% else %}
                    {% set split_table_name = table.identifier %}
                {% endif %}
                {% set table_name_in_source = source_table_prefix + '__' + split_table_name %}
                {% set cleanded_table_name = table_name_in_source | lower %}
                {{log(cleanded_table_name)}}
                {% for node in graph.sources.values() 
                    | selectattr("name", "equalto", cleanded_table_name)
                    | selectattr("source_name", "equalto", source_name) -%}
                    {{log('Found matching table from source: ' ~ node.name )}}
                        {% for column in node.columns.values()
                            | selectattr("meta.is_primary_key") %}
                            {%- do primary_keys.append(column.name) -%}
                        {% endfor %}
                {%- endfor %}
                {{log(primary_keys)}}
                {% if primary_keys|length == 1 %}
                    {% do model_yaml.append('              column_name: ' ~ primary_keys[0]) %}
                {% elif primary_keys|length > 1 %}
                    {% do model_yaml.append('              column_names:') %}
                    {% for key in primary_keys %}
                    {% do model_yaml.append('                - ' ~ key )%}
                    {% endfor %}
                {% endif %}
                {% if limit_pk_test %}
                    {% do model_yaml.append('              config:') %}
                    {% do model_yaml.append('                where: "dbt_valid_to is null"') %}
                {% endif %}

            {% endif %}
            {% do model_yaml.append('') %}
        {% set complete_model = model_yaml | join ('\n') %}     
        {% do final_yaml.append(complete_model) %}
    {% endfor %}

    {% if execute %}

        {% set joined = final_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}

    {% endif %}

{% endif %}

{% endmacro %}
