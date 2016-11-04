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



CREATE  TABLE  if not exists wlfinance.t_zlj_base_uid (
uid   String COMMENT '',
id1  String,
id2 String,
id3 String,
id4 String
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   LINES TERMINATED BY '\n'
stored as textfile ;

LOAD DATA  INPATH '/user/zlj/test_data.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_uid PARTITION (ds='zhaoshang_test_1103') ;

SELECT t1.id1 ,count(1) from wlfinance.t_zlj_base_uid t1 join t_base_uid_tmp t2 on t1.uid =t2.uid and t1.ds='zhaoshang_test_1103' and  t2.ds='ttinfo'
group by t1.id1





CREATE  TABLE  if not exists t_base_uid (
user_id   String COMMENT '',
cat_tag  ,
price_tag
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   LINES TERMINATED BY '\n'
stored as textfile ;