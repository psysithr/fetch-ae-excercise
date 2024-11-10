select 
_id::json->>'$oid' as receipt_id,
"bonusPointsEarned" as bonus_points_earned,
"bonusPointsEarnedReason" as bonus_points_earned_reason,
to_timestamp(cast("createDate"::json ->>'$date' as double precision)/1000) as create_date,
to_timestamp(cast("dateScanned"::json ->>'$date' as double precision)/1000) as date_scanned,
to_timestamp(cast("finishedDate"::json ->>'$date' as double precision)/1000) as finished_date,
to_timestamp(cast("modifyDate"::json ->>'$date' as double precision)/1000) as modify_date,
to_timestamp(cast("pointsAwardedDate"::json ->>'$date' as double precision)/1000) as points_awarded_date,
"pointsEarned"::decimal as points_earned,
to_timestamp(cast("purchaseDate"::json ->>'$date' as double precision)/1000) as purchase_date,
"purchasedItemCount"::decimal as purchased_item_count,
"rewardsReceiptItemList" as rewards_receipt_item_list,
"rewardsReceiptStatus" as rewards_receipt_status,
"totalSpent"::decimal as total_spent,
"userId" as user_id
from {{ source('raw_data', 'receipts') }}
