insert overwrite table wlservice.ppzs_brandid_weeksold PARTITION(ds='${hiveconf:yes_day}')
select brand_id,sum(weeksold) as weeksold from
(
select tt1.brand_id as brand_id ,tt1.item_id as item_id,
case
when tt2.item_id is null and tt1.now_sold is not null then tt1.now_sold
when tt1.now_sold is null then 0
when tt2.sold is null then 0
else tt1.now_sold - tt2.sold
END AS weeksold
FROM
(
select t1.item_id as item_id,t1.brand_id as brand_id,t2.sold as now_sold from
(select brand_id,item_id from wlservice.ppzs_itemid_info where ds = '20161127')t1
JOIN
(select item_id,sold from wlbase_dev.t_base_ec_shopitem_b where ds = '${hiveconf:yes_day}')t2
ON
t1.item_id = t2.item_id
)tt1
left JOIN
(
select item_id,sold from wlbase_dev.t_base_ec_shopitem_b where ds = '${hiveconf:last_day}'
)tt2
ON
tt1.item_id = tt2.item_id
)t
group by brand_id;
-- -----------------------------------------------------------
--
-- select tt1.item_id,tt1.brand_id,
-- case when
-- tt2.item_id is null then tt1.now_sold
-- else tt1.now_sold - tt2.now_sold
-- end
-- (
-- select t1.item_id as item_id,t1.brand_id as brand_id,t2.sold as now_sold from
-- (select brand_id,item_id from wlservice.ppzs_itemid_info where ds = 20161108 and brand_id = 105076)t1
-- JOIN
-- (select item_id,sold from wlbase_dev.t_base_ec_shopitem_b where ds = 20161106 and sold is not null)t2
-- ON
-- t1.item_id = t2.item_id
-- )tt1 -- this week
-- left JOIN
-- (
-- select t1.item_id as item_id,t1.brand_id as brand_id,t2.sold as now_sold from
-- (select brand_id,item_id from wlservice.ppzs_itemid_info where ds = 20161108 and brand_id = 105076)t1
-- JOIN
-- (select item_id,sold from wlbase_dev.t_base_ec_shopitem_b where ds = 20161030 and sold is not null)t2
-- ON
-- t1.item_id = t2.item_id
-- )tt2 -- last week
--
-- ------------------------------------------------------------
-- insert overwrite table wlservice.ppzs_brandid_weeksold PARTITION(ds='20161106')
-- select t1.brand_id,sum(t2.week_sold) as weeksold from
-- (select brand_id,item_id from wlservice.ppzs_itemid_info where ds = 20161108)t1
-- JOIN
-- (
-- select item_id,sum(day_sold) as week_sold from wlbase_dev.t_base_ec_item_daysale_dev_new where ds >= 20161030 and ds <=20161108
-- group by item_id
-- )t2
-- ON
-- t1.item_id = t2.item_id
-- group by
-- t1.brand_id
--
-- --------------------------------------------------------------
-- --终极填坑版：
-- insert overwrite table wlservice.ppzs_brandid_weeksold PARTITION(ds='20161106')
-- select brand_id,sum(week_sold) as weeksold from
-- (
-- select tt1.item_id as item_id ,tt1.brand_id as brand_id,
-- case
-- WHEN tt1.now_sold is null or tt2.last_sold is null then 0
-- when tt2.item_id is null then tt1.now_sold
-- when tt1.now_sold - tt2.last_sold < 0 then 0
-- else tt1.now_sold - tt2.last_sold
-- end
-- as week_sold from
-- (
-- select t1.item_id as item_id,t1.brand_id as brand_id,t2.sold as now_sold from
-- (select brand_id,item_id from wlservice.ppzs_itemid_info where ds = 20161108)t1
-- JOIN
-- (
-- select item_id,max(sold) as sold from wlbase_dev.t_base_ec_shopitem_b where ds <= 20161106 and ds >= 20161031
-- group by item_id
-- )t2
-- ON
-- t1.item_id = t2.item_id
-- )tt1 -- this week
-- left JOIN
-- (
-- select t1.item_id as item_id,t1.brand_id as brand_id,t2.sold as last_sold from
-- (select brand_id,item_id from wlservice.ppzs_itemid_info where ds = 20161108)t1
-- JOIN
-- (
-- select item_id,max(sold) as sold from wlbase_dev.t_base_ec_shopitem_b where ds <= 20161030 and ds >= 20161024
-- group by item_id
-- )t2
-- ON
-- t1.item_id = t2.item_id
-- )tt2 -- last week
-- ON
-- tt1.item_id = tt2.item_id
-- )T
-- group by brand_id
--
-- ----------------------------------------------------------------------------------
-- select tt1.item_id,tt1.now_sold,tt2.last_sold from
-- (
-- select t1.item_id as item_id,t1.brand_id as brand_id,t2.sold as now_sold from
-- (select brand_id,item_id from wlservice.ppzs_itemid_info where ds = 20161108)t1
-- JOIN
-- (
-- select item_id,max(sold) as sold from wlbase_dev.t_base_ec_shopitem_b where ds <= 20161106 and ds >= 20161031
-- group by item_id
-- )t2
-- ON
-- t1.item_id = t2.item_id
-- )tt1 -- this week
-- left JOIN
-- (
-- select t1.item_id as item_id,t1.brand_id as brand_id,t2.sold as last_sold from
-- (select brand_id,item_id from wlservice.ppzs_itemid_info where ds = 20161108)t1
-- JOIN
-- (
-- select item_id,max(sold) as sold from wlbase_dev.t_base_ec_shopitem_b where ds <= 20161030 and ds >= 20161024
-- group by item_id
-- )t2
-- ON
-- t1.item_id = t2.item_id
-- )tt2 -- last week
-- ON
-- tt1.item_id = tt2.item_id
-- where tt1.brand_id = 14601550

