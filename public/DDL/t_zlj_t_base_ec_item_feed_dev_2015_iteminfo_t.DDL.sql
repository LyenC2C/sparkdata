CREATE EXTERNAL TABLE  if not exists t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t (
item_id STRING ,
feed_id STRING ,
user_id STRING ,
content_length BIGINT ,
annoy BIGINT ,
ds STRING ,
datediff BIGINT,
title STRING,
cat_id STRING ,
root_cat_id STRING,
root_cat_name  STRING ,
brand_id STRING ,
brand_name  STRING,
bc_type STRING,
price double  ,
location STRING
)
COMMENT '评价 商品详情join表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile location '/hive/warehouse/wlbase_dev.db/t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t/';

