CREATE EXTERNAL TABLE  if not exists t_base_ec_item_dev (
item_id STRING,
title STRING,
cat_id STRING,
cat_name STRING,
wine_cate STRING,
is_jinkou STRING,
brand_id STRING,
brand_name STRING,
bc_type STRING,
xiangxing STRING,
dushu STRING,
jinghan STRING,
price STRING,
price_zone STRING,
favor (bigint)
seller_id STRING,
shop_id STRING,
location STRING,
ts STRING,
ds STRING
)
COMMENT '电商商品基础信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;
