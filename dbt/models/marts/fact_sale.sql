-- Fact table: one row per property sale.
-- Holds the measure (price) plus foreign keys pointing to each dimension.
-- The key expressions MUST match the ones in dim_location and dim_property
-- exactly, otherwise the joins will not line up.

select
    transaction_id,
    -- Foreign key to dim_date
    date_of_transfer as date_day,
    -- Foreign key to dim_location (same hash as dim_location.location_key)
    md5(
        coalesce(postcode_district, '') || '|'
        || coalesce(town_city, '') || '|'
        || coalesce(district, '') || '|'
        || coalesce(county, '')
    ) as location_key,
    -- Foreign key to dim_property (same hash as dim_property.property_key)
    md5(
        coalesce(property_type, '') || '|'
        || coalesce(is_new_build::text, '') || '|'
        || coalesce(tenure, '')
    ) as property_key,
    -- Full postcode kept on the fact for detailed lookups
    postcode,
    -- The measure: what the property sold for
    price,
    -- A = standard sale, B = additional (repossessions, buy-to-let etc.)
    ppd_category_type,
    -- Lineage back to the load batch that brought this row in
    batch_id
from {{ ref('stg_price_paid') }}