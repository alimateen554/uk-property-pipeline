-- Staging model: a thin, tidy layer over the loaded sales data.
-- Built as a VIEW (see dbt_project.yml), so it stores no data itself.
-- All mart models read from here, never from the raw table directly.

select
    transaction_id,
    price,
    date_of_transfer,
    postcode,
    postcode_district,
    property_type,
    property_type_desc,
    is_new_build,
    tenure,
    town_city,
    district,
    county,
    ppd_category_type,
    batch_id
-- source() points at the table declared in _sources.yml and
-- lets dbt record the lineage from raw table to model
from {{ source('staging', 'price_paid') }}