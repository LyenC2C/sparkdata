CREATE EXTERNAL TABLE  if not exists t_base_ec_sku (
propid STRING  COMMENT '����id' ,
propname STRING  COMMENT '����name' ,
valueid STRING  COMMENT '����ֵid' ,
valuename STRING  COMMENT '����ֵname' ,
ts  String,
tmp1 String,
tmp2 String

)
COMMENT 'sku�����'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n';
-- stored as textfile location '/hive/external/wlbase_dev/t_base_ec_shop_dev' ;
-- LOAD DATA  INPATH '/commit/t_base_ec_shop_dev' OVERWRITE INTO TABLE t_base_ec_shop_dev PARTITION (ds='20160216') ;