-- Location dimension: one row per unique combination of
-- postcode district, town, district and county.

select distinct
    -- Surrogate key: an md5 hash of the four location columns.
    -- coalesce turns NULLs into empty strings so the hash is never NULL,
    -- and the '|' separators keep different combinations distinct.
    md5(
        coalesce(postcode_district, '') || '|'
        || coalesce(town_city, '') || '|'
        || coalesce(district, '') || '|'
        || coalesce(county, '')
    ) as location_key,
    postcode_district,
    town_city,
    district,
    county
from {{ ref('stg_price_paid') }}