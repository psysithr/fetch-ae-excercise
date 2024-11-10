select 
_id::json->>'$oid' as receipt_id,
to_timestamp(cast("purchaseDate"::json ->>'$date' as double precision)/1000) as purchase_date,
json_array_elements("rewardsReceiptItemList"::json)->>'brandCode' as brand_code,
json_array_elements("rewardsReceiptItemList"::json)->>'barcode' AS barcode,
(json_array_elements("rewardsReceiptItemList"::json)->>'quantityPurchased')::int AS quantity_purchased,
json_array_elements("rewardsReceiptItemList"::json)->>'rewardsProductPartnerId' AS rewards_product_partner_id,
(json_array_elements("rewardsReceiptItemList"::json)->>'itemPrice')::decimal AS item_price
from {{ source('raw_data', 'receipts') }}
