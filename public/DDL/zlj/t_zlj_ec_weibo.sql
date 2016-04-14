CREATE  TABLE  if not exists t_base_ec_weibo (
id   String COMMENT '',
weibo  String ,
stars  int
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;