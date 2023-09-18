{% macro generate_stg_models(source_name) %}

{% set source_name = ""~ source_name ~"" %}

{% set source_nodes_to_generate = [] %}
{% set source_table_names = [] %}

{% for node in graph.sources.values() 
            | selectattr("resource_type", "equalto", "source") %}
            {% if node.source_name == source_name %}
                {% do source_nodes_to_generate.append(node) %}
                {% do source_table_names.append(node.name) %}
            {% endif %}
{% endfor %}

{% set zsh_command_models = "source dbt_packages/generation_macros/bash_scripts/stg_creation.sh """~ source_name ~""" " %}

{%- set models_array = [] -%}

{% for t in source_table_names %}
    {% set help_command = zsh_command_models + t %}
    {{ models_array.append(help_command) }}
{% endfor %}

{{ log("Run these commands in your shell to generate the stg models:\n" ~ models_array|join(' && \n'), info=True) }}

{% endmacro %}
