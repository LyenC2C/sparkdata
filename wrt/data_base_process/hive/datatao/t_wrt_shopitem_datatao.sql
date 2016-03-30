create table t_wrt_shopitem_datatao(
shop_id String,
shop_name String,
item_id String,
item_title String,
r_price String,
s_price String,
quantity String
);

insert overwrite table t_wrt_shopitem_datatao
select
t1.shop_id,t2.shop_name,t1.item_id,t1.item_title,t1.r_price,t1.s_price,t1.quantity
from
(select * from t_base_ec_item_sale_dev where ds=20160225)t1
join
(select shop_id,shop_name from t_wrt_shopinfo_datatao)t2
on
t1.shop_id = t2.shop_id;

--cat | sort >


