CREATE  TABLE  if not exists t_base_ec_jd_dim (
cat1  bigint    COMMENT '一级类目id ',
cat1_name bigint COMMENT '一级类目名字' ,
cat2 bigint ,
cat2_name bigint ,
cat3 bigint ,
cat3_name bigint
)
COMMENT '京东电商类目数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;