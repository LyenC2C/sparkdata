CREATE TABLE  if not exists t_base_shop_source (
shop_id	 STRING  COMMENT  '店铺id',
source STRING COMMENT '店铺来源'
)
COMMENT '店铺来源表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as RCFile  ;

-- LOAD DATA  INPATH '/user/wrt/temp/shop_source' OVERWRITE INTO TABLE t_base_shop_source