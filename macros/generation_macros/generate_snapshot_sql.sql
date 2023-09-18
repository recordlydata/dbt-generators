{% macro generate_snapshot_sql(source_name, table_name) %}

{%- set source_relation = source(source_name, table_name) -%}

{%- set columns = adapter.get_columns_in_relation(source_relation) -%}
{% set column_names=columns | map(attribute='name') %}
{% set snapshot_sql %}

{{ "{% snapshot " ~ source_name ~ "_snapshot__" ~ table_name ~ " %}" }}

{% raw %}
    {{
        config(
            unique_key="surrogate_key",
            strategy="check",
            check_cols=["surrogate_key", "row_checksum"],
        )
    }}
{% endraw %}

with source as (

    select *,
        {{ '{{ dbt_utils.generate_surrogate_key(["PRIMARY_KEY_HERE"]) }} as surrogate_key,' }}
        {%raw%}{{
            dbt_utils.generate_surrogate_key(
                dbt_utils.get_filtered_columns_in_relation(
                    from=source('{%endraw%}{{source_name}}{%raw%}', '{%endraw%}{{table_name}}{%raw%}'), except=["META_COLUMNS"]
                )
            )
        }} as row_checksum{%endraw%}
    from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}

)
{%raw%}
{{
    dbt_utils.deduplicate(
        relation="source",
        partition_by="surrogate_key",
        order_by="surrogate_key",
    )
}}
{%endraw%}
{{ "{% endsnapshot %}" }}

{% endset %}

{% if execute %}

{{ log(snapshot_sql, info=True) }}
{% do return(snapshot_sql) %}

{% endif %}
{% endmacro %}


{% endmacro %}
