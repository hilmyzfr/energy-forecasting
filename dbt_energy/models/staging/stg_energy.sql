with source as (
    select * from {{ ref('energy') }}
),

renamed as (
    select
        cast(date as date)           as date,
        cast(consumption as double)  as consumption_gwh
    from source
    where consumption is not null
)

select * from renamed
order by date
