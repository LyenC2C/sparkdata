CREATE EXTERNAL TABLE  if not exists t_wrt_wine_ec_item_info_new (
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
favor STRING,
seller_id STRING,
shop_id STRING,
location STRING,
ts STRING
)
COMMENT '酒类商品详情表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;


-- LOAD DATA  INPATH '/user/wrt/wine_iteminfo_tmp' OVERWRITE INTO TABLE t_wrt_wine_ec_item_info_new PARTITION (ds='20160321');