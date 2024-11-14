select
    barcode,
    "brandCode" as brand_code,
    category,
    "categoryCode" as category_code,
    cpg,
    "topBrand" as top_brand,
    name,
    _id::json ->> '$oid' as _id
from {{ source('raw_data', 'brands') }}
