create table t_wrt_iteminfo_datatao(
item_id STRING,
title STRING,
cat_name STRING,
root_cat_name STRING,
price STRING,
price_zone STRING,
favor STRING,
brand_name STRING,
shop_id STRING,
location STRING
);

insert overwrite table t_wrt_iteminfo_datatao
select item_id,title,cat_name,root_cat_name,price,price_zone,favor,brand_name,shop_id,location from
t_base_ec_item_dev where ds = 20160225 and root_cat_id = 1801 and bc_type = 'B' limit 1000;

-- check count:
select count(1) from
(
select item_id,title,cat_name,root_cat_name,price,price_zone,favor,brand_name,shop_id,location from
t_base_ec_item_dev where ds = 20160225 and root_cat_id = 1801 and bc_type = 'B')t