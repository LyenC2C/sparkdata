--创建店铺月销量表
create table t_wrt_shop_monsold as
select t2.shop_id as shop_id,t1.mon as mon,sum(t1.day_sold) as monsales,sum(t1.day_sold_price) as monsold from
(select item_id,day_sold,day_sold_price,substr(ds,1,6) as mon from wlbase_dev.t_base_ec_item_daysale_dev_new )t1
join
(select item_id,shop_id from wlbase_dev.t_base_ec_item_dev_new where ds = 20160615)t2
on
t1.item_id = t2.item_id
group by t1.mon,t2.shop_id;