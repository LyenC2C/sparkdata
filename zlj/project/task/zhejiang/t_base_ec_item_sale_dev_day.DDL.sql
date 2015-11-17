

use wlbase_dev;
drop table   t_zlj_base_ec_item_sale_dev_day;

CREATE  TABLE  if not exists t_zlj_base_ec_item_sale_dev_day (
shop_id string ,
item_id string ,
title string ,
s_price FLOAT ,
day_sold int  ,
day_sold_price FLOAT ,
location string ,
cate_id string ,
cate_name  string ,
cate_level1_id  string ,
cate_level1_name string ,
brand_id string ,
brand_name  string,
bc_type String
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

