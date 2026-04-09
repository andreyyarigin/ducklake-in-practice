{% macro current_timestamp_utc() %}
    timezone('UTC', now())
{% endmacro %}


{% macro date_spine_days(start_date, end_date) %}
    {#
        Генерирует последовательность дат от start_date до end_date включительно.
        Используется в тестах и отчётах для обнаружения пропусков.
    #}
    with date_spine as (
        select
            ({{ start_date }}::date + interval (n || ' days'))::date as date_day
        from (
            select unnest(range(0, datediff('day', {{ start_date }}::date, {{ end_date }}::date) + 1)) as n
        )
    )
    select * from date_spine
{% endmacro %}


{% macro safe_divide(numerator, denominator, default=0) %}
    case
        when ({{ denominator }}) = 0 or ({{ denominator }}) is null
        then {{ default }}
        else ({{ numerator }})::float / ({{ denominator }})
    end
{% endmacro %}
