create table t_wrt_shopsale_datatao(
shop_id String,
total_sold String,
day_sold STRING,
day_sold_price STRING,
ds String
);

insert overwrite table t_wrt_shopsale_datatao
select /*+ mapjoin(t1)*/
t1.shop_id,sum(t1.total_sold) as total_sold,sum(t2.day_sold)as day_sold,sum(t2.day_sold_price) as day_sold_price,t1.ds from
(
select /*+ mapjoin(t_wrt_shopinfo_datatao)*/
tt.shop_id,tt.item_id,tt.total_sold,tt.ds FROM
t_wrt_shopinfo_datatao
JOIN
(
select shop_id,item_id,total_sold,ds from
t_base_ec_item_sale_dev where ds >= 20160301 and ds<= 20160310
)tt
ON
tt.shop_id = t_wrt_shopinfo_datatao.shop_id
)t1
join
(
select item_id,day_sold,day_sold_price,ds from
t_base_ec_item_daysale_dev where ds >= 20160301 and ds<= 20160310
)t2
ON
t1.item_id = t2.item_id and t1.ds = t2.ds
group by t1.shop_id,t1.ds;

-- python ../shopsale.py

-- insert overwrite table t_wrt_shopsale_datatao
-- select /*+ mapjoin(t_wrt_shopinfo_datatao)*/
-- t1.shop_id,t1.total_sold,t2.day_sold,t2.day_sold_price,t1.ds from
-- t_wrt_shopinfo_datatao
-- join
-- (select shop_id,sum(total_sold) as total_sold,ds from
-- t_base_ec_item_sale_dev where ds >= 20160301 and ds<= 20160310 group by shop_id,ds)t1
-- on
-- t1.shop_id = t_wrt_shopinfo_datatao.shop_id
-- JOIN
-- (
-- select shop_id,sum(day_sold) as day_sold,sum(day_sold_price) as day_sold_price,ds from
-- t_base_ec_item_daysale_dev where ds >= 20160301 and ds<= 20160310 group by shop_id,ds
-- )t2
-- ON
-- t2.shop_id = t_wrt_shopinfo_datatao.shop_id and t2.ds = t1.ds;