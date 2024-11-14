-- This model is used to answer questions about brand ranking

-- Ideally we would use barcode to join brands_clean to items_purchased
-- However, due to the data quality issues identified, we will attempt
--      to use various keys to join the two models

-- There are cases where the cpg id can be used to join the two models
-- This can only be used as a key if it is unique in brands_clean
with cpg as (
    select
        _id,
        (cpg::json ->> '$id')::json ->> '$oid' as cpg_id
    from {{ ref('brands_clean') }}
),

joinable_cpg as (
    select
        cpg_id,
        count(*)
    from cpg
    group by 1
    having count(*) = 1
),

joinable_cpg_brand as (
    select
        brand_code,
        (cpg::json ->> '$id')::json ->> '$oid' as cpg_id
    from {{ ref('brands_clean') }}
    inner join joinable_cpg
        on (cpg::json ->> '$id')::json ->> '$oid' = cpg_id
),

-- Similarly, we can only join using barcode as a key if it is unique in brands_clean
joinable_barcode as (
    select
        barcode,
        count(*)
    from {{ ref('brands_clean') }}
    group by 1
    having count(*) = 1
),

joinable_barcode_brand as (
    select
        barcode,
        brand_code
    from {{ ref('brands_clean') }}
    inner join joinable_barcode
        using (barcode)
),

items_with_brand as (
    select
        ip.*,
        b1.brand_code as b1_brand_code,
        b2.brand_code as b2_brand_code
    from {{ ref('items_purchased') }} as ip
    left join joinable_barcode_brand as b1 on ip.barcode = b1.barcode
    left join
        joinable_cpg_brand as b2
        on ip.rewards_product_partner_id = b2.cpg_id
),

brand_metrics_by_month as (
    select
        date(date_trunc('month', purchase_date)) as purchase_month,
        coalesce(
            brand_code, b1_brand_code, b2_brand_code, 'no matching brand'
        ) as brand_code,
        count(distinct receipt_id) as receipts_scanned,
        avg(item_price) as avg_spend,
        sum(item_price) as total_spend,
        sum(quantity_purchased) as items_purchased
    from items_with_brand
    group by 1, 2
),

-- Calclate monthly rankings based on number of receipts scanned
receipts_scanned_ranked as (
    select
        *,
        'Receipts Scanned' as metric,
        row_number() over (
            partition by purchase_month
            order by receipts_scanned desc
        ) as rank
    from brand_metrics_by_month
    where brand_code != 'no matching brand'
),

-- Calclate monthly rankings based on number of avg spend
avg_spend_ranked as (
    select
        *,
        'Average Item Spend' as metric,
        row_number() over (
            partition by purchase_month
            order by avg_spend desc
        ) as rank
    from brand_metrics_by_month
    where brand_code != 'no matching brand'
),

-- Calclate monthly rankings based on total spend
total_spend_ranked as (
    select
        *,
        'Total Spend' as metric,
        row_number() over (
            partition by purchase_month
            order by total_spend desc
        ) as rank
    from brand_metrics_by_month
    where brand_code != 'no matching brand'
),

-- Calclate monthly rankings based on number of items purchased
items_purchased_ranked as (
    select
        *,
        'Items Purchased' as metric,
        row_number() over (
            partition by purchase_month
            order by total_spend desc
        ) as rank
    from brand_metrics_by_month
    where brand_code != 'no matching brand'
),

-- Join all ranking ctes together
-- Limit to top 5 for each evaluation metric
unioned_rankings as (
    select * from receipts_scanned_ranked where rank <= 5
    union
    select * from avg_spend_ranked where rank <= 5
    union
    select * from total_spend_ranked where rank <= 5
    union
    select * from items_purchased_ranked where rank <= 5
)

select
    purchase_month,
    metric,
    max(case when rank = 1 then brand_code end) as rank_1_brand,
    max(case when rank = 2 then brand_code end) as rank_2_brand,
    max(case when rank = 3 then brand_code end) as rank_3_brand,
    max(case when rank = 4 then brand_code end) as rank_4_brand,
    max(case when rank = 5 then brand_code end) as rank_5_brand
from
    unioned_rankings
group by 1, 2
order by 1, 2
