-- QA Items Purchased

select count(1) from items_purchased ip;
-- Finding: 6941 distinct items purchsed

select count(1) from items_purchased ip where barcode is null and brand_code is null;
-- Finding: There are 2997 records (~43% of all items purchased) without a barcode or brand_code. I would expect this to be closer to 0

select count(distinct barcode) from items_purchased ip;
-- Finding: There are 568 distinct barcodes

select count(distinct barcode) from items_purchased ip
where barcode not in (select distinct barcode from brands_clean bc);
-- Finding: The vast majority of items purchased do not have a matching barcode in brands_clean.
--			This is an issues because this is how we join brands_clean to items_purchsed

select count(1) from items_purchased where user_id is null;
-- Finding: All records have user_id populated. This is what I would expect

select count(distinct user_id) from items_purchased ip;
-- Finding: There are 246 distinct users who purchased items

select count(distinct user_id) from items_purchased ip
where user_id not in (select distinct user_id from users_clean uc);
-- Finding: 113 Users who purchased items don't have a matching user_id in users_clean, I would expect this to be closer to 0

select role, count(1), count(1)/sum(count(1)) over()
from items_purchased ip 
left join users_clean uc 
using(user_id)
group by 1;
-- Finding: Nearly 30% of items purchased were by users with the role fetch-staff. 
--			Are these actual purchases or is this test data?

select 'items_purchased' as source, sum(quantity_purchased) as n_purchased from items_purchased ip
union
select 'receipts_clean' as source, sum(purchased_item_count) as n_purchased from receipts_clean rc;
-- Finding: The number of items purchased does not align between items_purchased and receipts_clean

select 'items_purchased' as source, sum(item_price) as total_spend from items_purchased ip
union
select 'receipts_clean' as source, sum(total_spent) as total_spend from receipts_clean rc;
-- Finding: Total spend does not align between items_purchased and receipts_clean

select min(quantity_purchased) as min_items_purchased, max(quantity_purchased) as max_items_purchased, 
min(item_price) as min_item_price, max(item_price) as max_item_price from items_purchased ip;
-- Finding: The range of items purchased and prices seems reasonable except for items that have price of 0

select count(1) from items_purchased ip where item_price  = 0;
-- Finding: Only 4 records have 0 price

select count(1) from items_purchased ip where item_price is null;
-- Finding: 174 records have no price

select count(1) from items_purchased ip where quantity_purchased is null;
-- Finding: 174 records have no quantity



-- QA Receipts Clean

select count(1) from receipts_clean rc where bonus_points_earned_reason is not null and bonus_points_earned  is null;
-- Finding: If bonus_points_earned_reason is populated, bonus_points_earned is also populated. This is what I would expect

select points_awarded_date is null as null_points_awarded_date, bonus_points_earned is null as null_points_earned_date, count(1) from receipts_clean rc group by 1,2;
-- Finding: There are 65 records where there are points awarded but no associated rewarded date
-- 			Similarly there are 58 records where there is an awarded date but no points
--			I would expect both fields to be null or both fields to be not null

select count(1) from receipts_clean rc where date_scanned is null;
-- Finding: Date scanned is always populated. This is what I would expect

select count(1) from receipts_clean rc where purchase_date is null;
-- Finding: There are 448 receipts with null purchase date. I would expect purchase_date to always be populated

select extract(day from date_scanned-purchase_date), count(1), count(1)/sum(count(1)) over() from receipts_clean rc where date_scanned != purchase_date
group by 1
order by 1;
-- Finding: About 70% of the tie, the scan date and purchase date are within a day of each other.
--			There are many records where the number of days between these two dates is surprising

select receipt_id, count(1) from receipts_clean rc group by 1 having count(1) >1 ;
-- Finding: Receipt_id is a unique identifier. This is what I would expect

select min(bonus_points_earned), max(bonus_points_earned) from receipts_clean rc;
-- Finding: The range of points earned seems reasonable

select date_trunc('month', purchase_date)::date, 
count(1) as n_receipts,
sum(purchased_item_count) as total_purchased_items,
sum(total_spent) as total_spend,
case when sum(purchased_item_count) > 0 then sum(total_spent)/sum(purchased_item_count) else 0 end as avg_spend_per_item,
sum(purchased_item_count)/count(1) as avg_items_per_receipt,
sum(total_spent)/count(1) as avg_spend_per_receipt
from receipts_clean rc 
group by 1
order by 1;
-- Finding: It appears this data is incomplete. There are many months that have no receipt volume, and there are months where the volume seems low compared to other months.
--			For example, there are 9 receipts in total for the months 202009-202011
--			Additionally, there is much more variation in monthly avg spend per item, avg items per receipt, and avg spend per receipt than I would have expected
--			For example, receipts in January 2021 have much higher avg spend per receipt compared to the months immediately before and after
--			Additionally, December 2020 and February 2021 have similar avg spend per receipt, but very different spend per item and items per receipt



-- QA Brands Clean

select count(1), count(distinct _id), count(distinct barcode) from brands_clean bc;
-- Finding: _id is unique, which is what I would expect. However I was not expecting distinct _ids to have the same barcode

select count(1) from brands_clean bc 
where brand_code is null;
-- Finding: There are 234 records wher brand_code is null. I would expect this number to be 0

select name ilike('%test%') as is_test_brand, count(1), count(1), count(1)/sum(count(1)) over() as pct from brands_clean bc 
group by 1;
-- Finding: It looks like nearly 40% of brands are "test" brands and not actual brands



--QA Users Clean

select state, count(1), count(1)/sum(count(1)) over() as pct from users_clean uc group by 1;
-- Finding: 11% of users don't have a state. I would expect this to be closer to 0
--			80% of users are in WI. Also, there are many states without users. I'm unsure if this makes sense conceptually

select active, count(1) from users_clean uc group by 1;
-- Finding: There is only 1 user that is not active

select role, count(1), count(1)/sum(count(1)) over() as pct from users_clean uc group by 1;
-- Finding: 16% of users have a role of fetch-staff. 

select date_trunc('month', created_date)::date, count(1) from users_clean uc group by 1 order by 1;
-- Finding: The users data appears to be incomplete. There is a 2 year gap where no users were created
--			Additionally, the majority of users were created in January of 2021

select date_trunc('month', last_login)::date, count(1), count(1)/sum(count(1)) over() from users_clean uc group by 1 order by 1;
-- Finding: It is a bit surprising that last login date is so mch more heavily concentrated in older months
--			If we assume that this data is as of the end March 2021, only 8% of users have logged in during the past month
--			This could be explained if users are not automatically signed out of sessions
--			12% of users have no last logged in date. It's unclear if this means they created an account and never logged in
--				or if this is a data error










