CREATE  TABLE  if not exists t_base_ec_hanguo (
pre_sold   String COMMENT '',
root_cat  String ,
brand  String,
price String ,
comment_count String ,
key1 String ,
title String ,
item_id  String ,
category_final  String ,
site String ,
rank  String
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;