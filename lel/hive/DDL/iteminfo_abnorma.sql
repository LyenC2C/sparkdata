CREATE TABLE  if not exists t_base_ec_item_abnormal(
item_id STRING  COMMENT  '商品id',
title  STRING   COMMENT '商品title',
cat_id STRING  COMMENT '商品所属类目id',
cat_name STRING  COMMENT '商品所属类目名称',
root_cat_id STRING  COMMENT '商品顶级类目id',
root_cat_name STRING  COMMENT '商品顶级类目名称',
brand_id STRING COMMENT '品牌id',
bc_type STRING COMMENT '淘宝C 天猫B',
price STRING COMMENT '商品价格',
price_zone STRING  COMMENT '商品价格区间',
off_time STRING COMMENT '下架时间',
favor BIGINT COMMENT '收藏人数',
seller_id STRING  COMMENT '店家id',
shop_id STRING  COMMENT '店铺id',
location  String COMMENT '店铺地址' ,
sku  map<string, string>  COMMENT'sku  21433_89866585|1627207_3232479:price_quant,21433_89866585|1627207_3232479:price_quant',
ts STRING COMMENT '采集时间戳'
)
COMMENT '电商商品基础信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   COLLECTION ITEMS TERMINATED BY ','   MAP KEYS TERMINATED BY ':' ;