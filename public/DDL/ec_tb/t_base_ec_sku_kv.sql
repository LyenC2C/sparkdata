CREATE EXTERNAL TABLE  if not exists t_base_ec_sku (
propid STRING  COMMENT '属性id' ,
propname STRING  COMMENT '属性name' ,
valueid STRING  COMMENT '属性值id' ,
valuename STRING  COMMENT '属性值name' ,
ts  String,
tmp1 String,
tmp2 String

)
COMMENT 'sku详情表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n';
-- stored as textfile location '/hive/external/wlbase_dev/t_base_ec_shop_dev' ;
-- LOAD DATA  INPATH '/commit/t_base_ec_shop_dev' OVERWRITE INTO TABLE t_base_ec_shop_dev PARTITION (ds='20160216') ;