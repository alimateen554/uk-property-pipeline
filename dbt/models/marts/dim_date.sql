-- Date dimension: one row per calendar day, covering every day from
-- the first sale to the last. Built from a generated date range rather
-- than from the sales themselves, so days with no sales still exist
-- (important for time-series charts with no gaps).

with bounds as (
    -- Find the first and last sale dates in the data
    select
        min(date_of_transfer) as start_date,
        max(date_of_transfer) as end_date
    from {{ ref('stg_price_paid') }}
)

select
    -- The natural key: the date itself
    day::date as date_day,
    extract(year from day)::int as year,
    extract(quarter from day)::int as quarter,
    extract(month from day)::int as month,
    -- to_char pads month names with spaces, so trim removes them
    trim(to_char(day, 'Month')) as month_name,
    -- ISO day of week: 1 = Monday ... 7 = Sunday
    extract(isodow from day)::int as day_of_week,
    extract(isodow from day) in (6, 7) as is_weekend
from bounds
-- generate_series creates one row per day between the two dates
cross join generate_series(bounds.start_date, bounds.end_date, interval '1 day') as day