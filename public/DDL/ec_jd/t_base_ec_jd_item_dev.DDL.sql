
CREATE EXTERNAL TABLE  if not exists t_base_ec_item_dev (
item_id STRING  COMMENT  '商品id',
title  STRING   COMMENT '商品title',


price STRING COMMENT '商品价格',
price_zone STRING  COMMENT '商品价格区间',
is_online BIGINT COMMENT '1架上 0下架',
off_time STRING COMMENT '下架时间',
favor BIGINT COMMENT '收藏人数',
seller_id STRING  COMMENT '店家id',
shop_id STRING  COMMENT '店铺id',
location  String COMMENT '地址' ,
ts STRING COMMENT '采集时间戳'  ,

brand_id STRING COMMENT '品牌id',
brand_name STRING COMMENT '品牌名称',
cat1  String,
cat1_name String ,
cat2 String ,
cat2_name String ,
cat3 String ,
cat3_name String ,
u_jd String ,  -- 京东自营
table_list  map<string, string>   COMMENT '商品参数表' ,
paramap map<string, string>   COMMENT '商品参数表'
)
COMMENT '京东电商商品基础信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'    COLLECTION ITEMS TERMINATED BY ','   MAP KEYS TERMINATED BY ':'  ;
-- stored as textfile location '/hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/';


--ALTER TABLE t_base_ec_item_dev ADD COLUMNS  (paramap map<string, string>   COMMENT '商品参数表');

-- LOAD DATA  INPATH '/commit/itemdata' OVERWRITE INTO TABLE t_base_ec_item_dev PARTITION (ds='20160216') ;

-- INSERT overwrite table t_base_ec_item_dev partition(ds) select * from t_base_ec_item_dev_back where ds=20151101