{% macro generate_stg_sql(source_name, table_name) %}

{%- set source_relation = source(source_name, table_name) -%}

{%- set columns = adapter.get_columns_in_relation(source_relation) -%}
{% set column_names=columns | map(attribute='name') %}
{% set stg_sql %}

with snp as (

    select *
    from {% raw %}{{ ref({% endraw %}'{{ source_name }}_snapshot__{{ table_name }}'{% raw %}) }}{% endraw %}
    where dbt_valid_to is null

),
renamed as (

    select
        {%- if leading_commas -%}
        {%- for column in column_names %}
        {{", " if not loop.first}}{% if not case_sensitive_cols %}{{ column | lower }}{% elif target.type == "bigquery" %}{{ column }}{% else %}{{ "\"" ~ column ~ "\"" }}{% endif %}
        {%- endfor %}
        {%- else -%}
        {%- for column in column_names %}
        {% if not case_sensitive_cols %}{{ column | lower }}{% elif target.type == "bigquery" %}{{ column }}{% else %}{{ "\"" ~ column ~ "\"" }}{% endif %}{{"," if not loop.last}}
        {%- endfor -%}
        {%- endif %}

    from snp

)

select * from renamed

{% endset %}

{% if execute %}

{{ log(stg_sql, info=True) }}
{% do return(stg_sql) %}

{% endif %}
{% endmacro %}


{% endmacro %}
