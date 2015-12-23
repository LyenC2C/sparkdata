CREATE  TABLE  if not exists t_base_uid (
uid   String COMMENT ''
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;




CREATE  TABLE  if not exists t_base_uid (
user_id   String COMMENT '',
cat_tag  ,
price_tag
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   LINES TERMINATED BY '\n'
stored as textfile ;