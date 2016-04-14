CREATE  TABLE  if not exists t_base_uid (
uid   String COMMENT '',
id1  String,
id2 String,
id3 String,
id4 String
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