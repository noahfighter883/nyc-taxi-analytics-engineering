-- Static date spine covering a few years around the loaded trip months --
-- wider than strictly needed so new months can be added without having to
-- extend this range every time. See dbt_utils.date_spine docs for the
-- macro this generates SQL from.

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}

)

select
    date_day,
    extract(year from date_day)                    as year,
    extract(month from date_day)                    as month,
    extract(day from date_day)                      as day_of_month,
    dayname(date_day)                                as day_name,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend

from spine
