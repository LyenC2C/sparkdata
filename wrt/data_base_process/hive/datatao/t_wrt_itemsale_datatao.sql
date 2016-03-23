create table t_wrt_itemsale_datatao(
item_id STRING,
total_sold STRING,
day_sold STRING,
day_sold_price STRING,
ds STRING
);


insert overwrite table t_wrt_itemsale_datatao
select
t_wrt_iteminfo_datatao.item_id,t1.total_sold,t2.day_sold,t2.day_sold_price,t1.ds from
t_wrt_iteminfo_datatao
join
(select item_id,total_sold,ds from t_base_ec_item_sale_dev where ds >= 20160301 and ds <= 20160310 )t1
on
t_wrt_iteminfo_datatao.item_id = t1.item_id
join
(select item_id,day_sold,day_sold_price,ds from t_base_ec_item_daysale_dev where ds >= 20160301 and ds <= 20160310 )t2
on
t_wrt_iteminfo_datatao.item_id = t2.item_id and t1.ds = t2.ds;