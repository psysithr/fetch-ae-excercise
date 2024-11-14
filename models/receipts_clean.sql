select
    "bonusPointsEarned" as bonus_points_earned,
    "bonusPointsEarnedReason" as bonus_points_earned_reason,
    "pointsEarned"::decimal as points_earned,
    "purchasedItemCount"::decimal as purchased_item_count,
    -- "rewardsReceiptItemList" as rewards_receipt_item_list,
    "rewardsReceiptStatus" as rewards_receipt_status,
    "totalSpent"::decimal as total_spent,
    "userId" as user_id,
    _id::json ->> '$oid' as receipt_id,
    to_timestamp(
        ("createDate"::json ->> '$date')::double precision / 1000
    ) as create_date,
    to_timestamp(
        ("dateScanned"::json ->> '$date')::double precision / 1000
    ) as date_scanned,
    to_timestamp(
        ("finishedDate"::json ->> '$date')::double precision / 1000
    ) as finished_date,
    to_timestamp(
        ("modifyDate"::json ->> '$date')::double precision / 1000
    ) as modify_date,
    to_timestamp(
        ("pointsAwardedDate"::json ->> '$date')::double precision / 1000
    ) as points_awarded_date,
    to_timestamp(
        ("purchaseDate"::json ->> '$date')::double precision / 1000
    ) as purchase_date
from {{ source('raw_data', 'receipts') }}
