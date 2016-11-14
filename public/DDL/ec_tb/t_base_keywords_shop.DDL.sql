CREATE EXTERNAL TABLE  if not exists t_base_keywords_shop (
shop_id STRING  COMMENT '店铺id' ,
bc_type STRING COMMENT '店铺类型' ,
sold STRING COMMENT '店铺销量'
)
COMMENT '电商店铺基础信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n';

-- LOAD DATA  INPATH '/user/wrt/keywords_shop_tmp' OVERWRITE INTO TABLE t_base_keywords_shop PARTITION (ds='20160714');