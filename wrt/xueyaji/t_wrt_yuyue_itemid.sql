create table t_wrt_yuyue_itemid_price as
select item_id,price from wlbase_dev.t_base_ec_item_dev_new where ds = 20160731 and brand_name = '鱼跃' and (cat_id = 122630008 or cat_id = 122724004 or cat_id = 50023745);
--------------------------------------

create table t_wrt_yuyue_11_12_sold
as
select tt1.item_id,
case when tt2.sold_11 is null then tt1.sold_12
else tt1.sold_12 - tt2.sold_11 end as sold_2months
from
(
select t1.item_id,t2.total_sold as sold_12 from
(select item_id from t_wrt_yuyue_itemid_price)t1
join
(select item_id,total_sold from wlbase_dev.t_base_ec_item_sale_dev_new where  ds = 20151231)t2
on
t1.item_id = t2.item_id
)tt1
left join
(select item_id,total_sold as sold_11 from wlbase_dev.t_base_ec_item_sale_dev_new where  ds = 20151102)tt2
on
tt1.item_id = tt2.item_id;

create table t_wrt_yuyue_11_08_sold
as
select tt1.item_id,
case when tt2.sold_11 is null then tt1.sold_08
else tt1.sold_08 - tt2.sold_11 end as sold_11_08
from
(
select t1.item_id,t2.total as sold_08 from
(select item_id from t_wrt_yuyue_itemid_price)t1
join
(select item_id,total from wlbase_dev.t_base_ec_item_sold_dev where  ds = 20160729)t2
on
t1.item_id = t2.item_id
)tt1
left join
(select item_id,total_sold as sold_11 from wlbase_dev.t_base_ec_item_sale_dev_new where  ds = 20151102)tt2
on
tt1.item_id = tt2.item_id;


---------------------------------

create table t_wrt_yuyue_01_10_sold AS
select
t1.item_id,t2.sold as sold_01_12 from
(select item_id from t_wrt_yuyue_itemid_price)t1
JOIN
(select item_id,count(1) as sold from wlbase_dev.t_base_ec_record_dev_new
where dsn > 20150000 and dsn <20151100 group by item_id)t2
ON
t1.item_id = t2.item_id;


create table t_wrt_yuyue_08_10_sold AS
select
t1.item_id,t2.sold as sold_01_12 from
(select item_id from t_wrt_yuyue_itemid_price)t1
JOIN
(select item_id,count(1) as sold from wlbase_dev.t_base_ec_record_dev_new
where dsn > 20150800 and dsn <20151100 group by item_id)t2
ON
t1.item_id = t2.item_id;





create table t_wrt_yuyue_2015_sold_sales as
select tt1.item_id,tt1.sold_2015,tt1.sold_2015 * CAST(tt2.price as float) as sales_2015 from
(
select
case when t1.item_id is null then t2.item_id else t1.item_id end as item_id,
case when t1.sold_01_12 is null then t2.sold_2months
when t2.sold_2months is null then t1.sold_01_12
else t1.sold_01_12 + t2.sold_2months end as sold_2015
from
(select item_id,sold_01_12 from t_wrt_yuyue_01_10_sold)t1
FULL JOIN
(select item_id,sold_2months from t_wrt_yuyue_11_12_sold)t2
ON
t1.item_id = t2.item_id
)tt1
JOIN
(
select item_id,price from t_wrt_yuyue_itemid_price
)tt2
ON
tt1.item_id = tt2.item_id;

create table t_wrt_yuyue_2016_sold_sales as
select tt1.item_id,tt1.sold_2016,tt1.sold_2016 * CAST(tt2.price as float) as sales_2016 from
(
select
case when t1.item_id is null then t2.item_id else t1.item_id end as item_id,
case when t1.sold_01_12 is null then t2.sold_11_08
when t2.sold_11_08 is null then t1.sold_01_12
else t1.sold_01_12 + t2.sold_11_08 end as sold_2016
from
(select item_id,sold_01_12 from t_wrt_yuyue_08_10_sold)t1
FULL JOIN
(select item_id,sold_11_08 from t_wrt_yuyue_11_08_sold)t2
ON
t1.item_id = t2.item_id
)tt1
JOIN
(
select item_id,price from t_wrt_yuyue_itemid_price
)tt2
ON
tt1.item_id = tt2.item_id;


-------------------------------

create table t_wrt_yuyue_2015_xilie_sold_sales as
select t1.item_id,
case
when t2.title like "%臂式%" then "sbs"
when t2.title like "%腕式%" then "ws"
when t2.title like "%台式%" then "ts"
else "else" end as xilie,
t1.sold_2015,t1.sales_2015 from
(select * from t_wrt_yuyue_2015_sold_sales)t1
JOIN
(select item_id,title from wlbase_dev.t_base_ec_item_dev_new where ds = 20160731)t2
ON
t1.item_id = t2.item_id;

select xilie,sum(sold_2015),sum(sales_2015) from t_wrt_yuyue_2015_xilie_sold_sales group by xilie;


create table t_wrt_yuyue_2016_xilie_sold_sales as
select t1.item_id,
case
when t2.title like "%臂式%" then "sbs"
when t2.title like "%腕式%" then "ws"
when t2.title like "%台式%" then "ts"
else "else" end as xilie,
t1.sold_2016,t1.sales_2016 from
(select * from t_wrt_yuyue_2016_sold_sales)t1
JOIN
(select item_id,title from wlbase_dev.t_base_ec_item_dev_new where ds = 20160731)t2
ON
t1.item_id = t2.item_id;

select xilie,sum(sold_2016),sum(sales_2016)from t_wrt_yuyue_2016_xilie_sold_sales group by xilie;

------------------------------------------------

create table t_wrt_yuyue_2015_xinghao_sold_sales as
select t1.item_id,
case
when t2.title like "%690D%" then "690D"
when t2.title like "%690B%" then "690B"
when t2.title like "%690E%" then "690E"
when t2.title like "%690A%" then "690A"
when t2.title like "%680E%" then "680E"
when t2.title like "%680A%" then "680A"
when t2.title like "%660A%" then "660A"
when t2.title like "%660E%" then "660E"
when t2.title like "%666E%" then "666E"
when t2.title like "%660D%" then "660D"
when t2.title like "%630D%" then "630D"
when t2.title like "%660A%" then "660A"
when t2.title like "%660E%" then "660E"
when t2.title like "%660C%" then "660C"
when t2.title like "%620A%" then "620A"
when t2.title like "%655A%" then "655A"
when t2.title like "%655B%" then "655B"
when t2.title like "%630A%" then "630A"
when t2.title like "%670D%" then "670D"
when t2.title like "%670A%" then "670A"
when t2.title like "%630B%" then "630B"
when t2.title like "%8300B%" then "8300B"
when t2.title like "%8100B%" then "8100B"
when t2.title like "%8800C%" then "8800C"
when t2.title like "%8100C%" then "8100C"
else "else" end as xinghao,
t1.sold_2015,t1.sales_2015 from
(select * from t_wrt_yuyue_2015_sold_sales)t1
JOIN
(select item_id,title from wlbase_dev.t_base_ec_item_dev_new where ds = 20160731)t2
ON
t1.item_id = t2.item_id;

select xinghao,sum(sold_2015),sum(sales_2015) from t_wrt_yuyue_2015_xinghao_sold_sales group by xinghao;

create table t_wrt_yuyue_2016_xinghao_sold_sales as
select t1.item_id,
case
when t2.title like "%690D%" then "690D"
when t2.title like "%690B%" then "690B"
when t2.title like "%690E%" then "690E"
when t2.title like "%690A%" then "690A"
when t2.title like "%680E%" then "680E"
when t2.title like "%680A%" then "680A"
when t2.title like "%660A%" then "660A"
when t2.title like "%660E%" then "660E"
when t2.title like "%666E%" then "666E"
when t2.title like "%660D%" then "660D"
when t2.title like "%630D%" then "630D"
when t2.title like "%660A%" then "660A"
when t2.title like "%660E%" then "660E"
when t2.title like "%660C%" then "660C"
when t2.title like "%620A%" then "620A"
when t2.title like "%655A%" then "655A"
when t2.title like "%655B%" then "655B"
when t2.title like "%630A%" then "630A"
when t2.title like "%670D%" then "670D"
when t2.title like "%670A%" then "670A"
when t2.title like "%630B%" then "630B"
when t2.title like "%8300B%" then "8300B"
when t2.title like "%8100B%" then "8100B"
when t2.title like "%8800C%" then "8800C"
when t2.title like "%8100C%" then "8100C"
else "else" end as xinghao,
t1.sold_2016,t1.sales_2016 from
(select * from t_wrt_yuyue_2016_sold_sales)t1
JOIN
(select item_id,title from wlbase_dev.t_base_ec_item_dev_new where ds = 20160731)t2
ON
t1.item_id = t2.item_id;

select xinghao,sum(sold_2016),sum(sales_2016) from t_wrt_yuyue_2016_xinghao_sold_sales group by xinghao;

