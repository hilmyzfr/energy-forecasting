with source as (
    select * from {{ ref('weather') }}
),

renamed as (
    select
        cast(date as date)            as date,
        cast(temperature as double)   as temperature_c
    from source
    where temperature is not null
)

select * from renamed
order by date
