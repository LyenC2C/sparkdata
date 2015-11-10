CREATE EXTERNAL TABLE  if not exists t_base_ec_shop_dev (
shop_id STRING  COMMENT '店铺id' ,
seller_id STRING  COMMENT '卖家id' ,
shop_name STRING  COMMENT '店铺名称' ,
seller_name STRING  COMMENT '卖家名称' ,
star STRING  COMMENT '星级，天猫为99' ,
credit STRING  COMMENT '信誉等级' ,
starts STRING COMMENT '开店时间',
bc_type STRING COMMENT '店铺类型',
item_count BIGINT COMMENT '商品总数',
fans_count BIGINT   COMMENT '粉丝数' ,
good_rate_p FLOAT   COMMENT '好评率' ,
weitao_id STRING  COMMENT '微淘id' ,
desc_score FLOAT  COMMENT '描述分' ,
service_score FLOAT  COMMENT '服务分' ,
wuliu_score FLOAT  COMMENT '物流分' ,
location  String COMMENT '地址' ,
ts STRING COMMENT '采集时间戳'
)
COMMENT '电商店铺基础信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n';
-- stored as textfile location '/hive/external/wlbase_dev/t_base_ec_shop_dev' ;
