with energy as (
    select * from {{ ref('stg_energy') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

joined as (
    select
        e.date,
        e.consumption_gwh,
        w.temperature_c,
        -- isodow: Mon=1 … Sun=7; subtract 1 to get Mon=0 … Sun=6 (matches pandas)
        cast(extract('isodow' from e.date) - 1 as integer) as day_of_week,
        cast(extract('month'  from e.date) as integer)     as month,
        cast(extract('year'   from e.date) as integer)     as year,
        case when extract('isodow' from e.date) in (6, 7) then 1 else 0 end as is_weekend
    from energy e
    left join weather w on e.date = w.date
),

with_lags as (
    select
        *,
        -- lag features
        lag(consumption_gwh, 1) over (order by date) as lag_1,
        lag(consumption_gwh, 7) over (order by date) as lag_7,

        -- rolling 7-day average (excluding current day — same as shift(1).rolling(7))
        avg(consumption_gwh) over (
            order by date
            rows between 8 preceding and 2 preceding
        ) as rolling_7_avg,

        -- rolling 7-day stddev
        stddev(consumption_gwh) over (
            order by date
            rows between 8 preceding and 2 preceding
        ) as rolling_7_std

    from joined
)

select * from with_lags
where lag_1      is not null
  and lag_7      is not null
  and rolling_7_avg is not null
order by date
