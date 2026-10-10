-- Property dimension: one row per unique combination of property type,
-- new-build flag and tenure. Small combinations of descriptive flags
-- like this are often called a "junk dimension".

select distinct
    -- Surrogate key built the same way as in dim_location.
    -- is_new_build is a boolean, so it is cast to text before hashing.
    md5(
        coalesce(property_type, '') || '|'
        || coalesce(is_new_build::text, '') || '|'
        || coalesce(tenure, '')
    ) as property_key,
    property_type,
    property_type_desc,
    is_new_build,
    tenure
from {{ ref('stg_price_paid') }}