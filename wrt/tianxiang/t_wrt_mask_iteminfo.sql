insert overwrite table t_wrt_mask_iteminfo partition (ds = 20160527)
select
item_id,
title,
cat_id,
cat_name,
root_cat_id,
root_cat_name,
brand_id,
brand_name,
bc_type,
price,
price_zone,
is_online,
off_time,
favor,
seller_id,
shop_id,
location,
paramap,
sku,
ts
from
wlbase_dev.t_base_ec_item_dev_new
where
ds = 20160513 and
(cat_id = 121408011
or
cat_id = 121474010);

record表建立:
create table t_wrt_mask_record like wlbase_dev.t_base_ec_record_dev_new

insert overwrite table t_wrt_mask_record
select * from wlbase_dev.t_base_ec_record_dev_new where cat_id = 121408011 or cat_id = 121474010;

销量表:
create table t_wrt_tianxiang_daysale
(
item_id string,
day_sold bigint,
day_sold_price float,
ds string
);


insert overwrite table t_wrt_tianxiang_daysale
select /*+ mapjoin(t2)*/
t2.item_id,t2.day_sold,t2.day_sold_price,t2.ds FROM
(select item_id from t_wrt_tianxiang_iteminfo)t1
JOIN
(select * from wlbase_dev.t_base_ec_item_daysale_dev)t2
ON
t1.item_id = t2.item_id;


商品表与店铺来源表结合:

create table t_wrt_tianxiang_iteminfo AS

--insert overwrite table t_wrt_tianxiang_iteminfo
select t1.item_id,t1.title,t1.cat_id,t1.cat_name,t1.root_cat_id,t1.root_cat_name,t1.brand_id,t1.brand_name,t1.bc_type,t1.price,
t1.shop_id,t2.shop_name,t2.desc_score,t2.service_score,t2.wuliu_score,t2.location,t3.source
from
(select * from wlbase_dev.t_base_ec_item_dev_new where ds = 20160513 and (cat_id = 121408011 or cat_id = 121474010))t1
left JOIN
(select * from wlbase_dev.t_base_ec_shop_dev where ds = 20160330)t2
on
t1.shop_id = t2.shop_id
left JOIN
(select shop_id,source from wlbase_dev.t_base_shop_source)t3
ON
t1.shop_id = t3.shop_id;



品牌排行:
--日期改成相应的日期就与时间对应了
select tt1.brand_id,tt1.brand_name,tt1.sold,(tt1.sold-tt2.sold)/tt2.sold as huanbi_sold,
tt1.sales,(tt1.sales-tt2.sales)/tt2.sales as huanbi_sales from
(
select t1.brand_id,max(t1.brand_name) as brand_name,sum(t2.day_sold) as sold,sum(t2.day_sold_price) as sales from
(select item_id,brand_id,brand_name from t_wrt_tianxiang_iteminfo)t1
JOIN
(select * from t_wrt_tianxiang_daysale where ds > 20160425)t2
ON
t1.item_id = t2.item_id
group by t1.brand_id
)tt1
JOIN
(
select t1.brand_id,sum(t2.day_sold) as sold,sum(t2.day_sold_price) as sales from
(select item_id,brand_id,brand_name from t_wrt_tianxiang_iteminfo)t1
JOIN
(select * from t_wrt_tianxiang_daysale where ds < 20160425 and ds >= 20160325 )t2
ON
t1.item_id = t2.item_id
group by t1.brand_id
)tt2
ON
tt1.brand_id = tt2.brand_id


渠道分析,销量与销售额:

select tt1.shop_id,tt1.shop_name as shop_name,tt1.location as location,(tt1.sold-tt2.sold)/tt2.sold as huanbi_sold,
(tt1.sales-tt2.sales)/tt2.sales as huanbi_sales, tt1.source as source from
(
select t1.shop_id,max(t1.shop_name) as shop_name,max(t1.location) as location,
max(t1.desc_score),max(t1.service_score),max(t1.wuliu_score),
sum(t2.day_sold) as sold,sum(t2.day_sold_price) as sales, t1.source as source from
(select * from t_wrt_tianxiang_iteminfo)t1
JOIN
(select * from t_wrt_tianxiang_daysale where ds > 20160425)t2
ON
t1.item_id = t2.item_id
group by t1.shop_id
)tt1
JOIN
(
select t1.shop_id,max(t1.shop_name) as shop_name,sum(t2.day_sold) as sold,sum(t2.day_sold_price) as sales from
(select item_id,shop_id,shop_name from t_wrt_tianxiang_iteminfo)t1
JOIN
(select * from t_wrt_tianxiang_daysale where ds < 20160425 and ds >= 20160325)t2
ON
t1.item_id = t2.item_id
group by t1.shop_id
)tt2
ON
tt1.shop_id = tt2.shop_id


----------------------------------------------------------

