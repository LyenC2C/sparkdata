

drop table t_zlj_base_match;


create table t_zlj_base_match(
tel string,
id1 string ,
id2 string ,
id3 string ,
id4 string ,
id5 string ,
id6 string ,
id7 string
)
COMMENT '商务匹配表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,'   LINES TERMINATED BY '\n'
stored as textfile ;

LOAD DATA   INPATH '/user/zlj/match/data.csv' OVERWRITE INTO TABLE t_zlj_base_match PARTITION (ds='rong360_test_1111') ;
LOAD DATA   INPATH '/user/zlj/match/rong360.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='ygz_part') ;



SELECT
COUNT(1)
 from  t_zlj_base_match t1 join wlbase_dev.t_base_yhhx_model_tel t2 on t1.ds='rong360_test_1111' and t1.tel=t2.uid ;
