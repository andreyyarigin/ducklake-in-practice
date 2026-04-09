{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Все модели пишутся в схему 'main' DuckLake (единственная схема).
        custom_schema_name игнорируется — нам не нужны dev/prod суффиксы
        внутри DuckLake.
    #}
    main
{%- endmacro %}
