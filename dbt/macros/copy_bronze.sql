-- in dbt/macros/copy_bronze.sql
{% macro copy_bronze(table_name, file_path) %}
COPY {{ this.schema }}.{{ table_name }}
FROM '{{ file_path }}'
CSV HEADER;
{% endmacro %}
