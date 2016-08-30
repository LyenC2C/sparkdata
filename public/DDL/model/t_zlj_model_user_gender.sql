

DROP TABLE t_zlj_model_user_gender ;
CREATE  TABLE  if not exists t_zlj_model_user_gender (

id bigint COMMENT ' 用户UID ' ,
gender int COMMENT ' 性别 '
)
COMMENT '微博用户信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;


LOAD DATA LOCAL   INPATH '/home/zlj/datas/t_zlj_model_user_gender_fix_v2'
OVERWRITE INTO TABLE t_zlj_model_user_gender PARTITION (ds='20160829')