create table t_base_ec_item_daysale_dev_v2 like t_base_ec_item_daysale_dev_new;

INSERT overwrite TABLE t_base_ec_item_daysale_dev_v2 PARTITION (ds)
select t1.item_id,t1.day_sold,
case when (t1.day_sold_price/t1.day_sold) - t2.price > 1000000 then t2.price * t1.day_sold
else t1.day_sold_price end,t1.ds as ds
from
(select * from t_base_ec_item_daysale_dev_new )t1

join

(select item_id,price from t_base_ec_item_dev_new where ds = 20160615)t2

ON
t1.item_id = t2.item_id;

create table t_wrt_tmp_110483934_itemgoucheng as
select tt1.item_id,tt2.day_sold/18707 from
(
  select item_id,sold from
  (select t1.item_id,sum(t1.day_sold) as sold from
  (select item_id,day_sold from t_base_ec_item_daysale_dev_new where ds > 20160500 and ds <20160600)t1
  join
  (select item_id,shop_id from t_base_ec_item_dev_new where ds = 20160615 and shop_id = 110483934)t2
  on
  t1.item_id = t2.item_id
  group by t1.item_id)t
  order by sold desc limit 10
)tt1
join
(select item_id,day_sold from t_base_ec_item_daysale_dev_new where ds > 20160500 and ds <20160600)tt2
on
tt1.item_id = tt2.item_id;


--

use wlservice;
create table t_wrt_tmp_3shop_totalsold AS
select /*+ mapjoin(t2)*/
t2.shop_id as shop_id,t1.item_id as item_id,t1.total as total,t2.price as price,t1.ds as ds from
(select item_id,total,ds from wlbase_dev.t_base_ec_item_sold_dev where cp_flag <> 1)t1
JOIN
(select item_id,shop_id,price,ds from wlbase_dev.t_base_ec_item_dev_new where ds = 20160615 and shop_id in ('57299948','104820621','103569798'))t2
ON
t1.item_id = t2.item_id;

create table t_wrt_tmp_3shop_totalsold_v3 as
select
/*+ mapjoin(t2)*/
t2.shop_id as shop_id,t1.item_id as item_id,t2.title as title,t1.day_sold as sold,t1.day_sold as sales,t1.ds as ds from
(select item_id,day_sold,day_sold_price,ds from wlbase_dev.t_base_ec_item_daysale_dev_new where ds > 20160400 and ds <20160600)t1
join
(select item_id,shop_id,price,title,ds from wlbase_dev.t_base_ec_item_dev_new where ds = 20160615 and shop_id in ('57299948','104820621','103569798'))t2

select item_id,title,sold,sales from
(
select t1.item_id,title,sum(sold) as sold,sum(sales) as sales FROM
t_wrt_tmp_3shop_totalsold_v3 where shop_id = 57299948 and ds > 20160500
group by item_id)t
order by sold desc limit 15;

create table t_wrt_3shop_title as
select item_id,title from wlbase_dev.t_base_ec_item_dev_new where ds = 20160615 and shop_id in ('57299948','104820621','103569798')

select * from ta