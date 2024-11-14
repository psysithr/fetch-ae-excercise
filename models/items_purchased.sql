select
    (
        json_array_elements("rewardsReceiptItemList"::json)
        ->> 'quantityPurchased'
    )::int as quantity_purchased,
    (
        json_array_elements("rewardsReceiptItemList"::json) ->> 'itemPrice'
    )::decimal as item_price,
    _id::json ->> '$oid' as receipt_id,
    "userId" as user_id,
    to_timestamp(
        ("purchaseDate"::json ->> '$date')::double precision / 1000
    ) as purchase_date,
    json_array_elements("rewardsReceiptItemList"::json)
    ->> 'brandCode' as brand_code,
    json_array_elements("rewardsReceiptItemList"::json)
    ->> 'barcode' as barcode,
    json_array_elements("rewardsReceiptItemList"::json)
    ->> 'rewardsProductPartnerId' as rewards_product_partner_id
from {{ source('raw_data', 'receipts') }}
