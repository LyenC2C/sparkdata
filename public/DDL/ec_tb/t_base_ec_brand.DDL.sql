CREATE  TABLE  if not exists t_base_ec_brand (
brand_id   String COMMENT 'id',
brand_name  String ,
item_count  bigint ,

avg_price
max_price
min_price
03price
05price
08price
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;