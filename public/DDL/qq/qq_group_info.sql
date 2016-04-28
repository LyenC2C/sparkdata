CREATE  TABLE  if not exists qq_group_info (
qun_id  String     COMMENT 'һ����Ŀid ',
qun_class  String     COMMENT 'һ����Ŀid ',
mast_qq  String     COMMENT 'һ����Ŀid ',
title  String     COMMENT 'һ����Ŀid ',
create_date  String     COMMENT 'һ����Ŀid ',
qun_text  String     COMMENT 'һ����Ŀid ',
find_schools String
)
COMMENT 'qq Ⱥ����'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;