CREATE  TABLE  if not exists qq_group_info (
qun_id  String     COMMENT '一级类目id ',
qun_class  String     COMMENT '一级类目id ',
mast_qq  String     COMMENT '一级类目id ',
title  String     COMMENT '一级类目id ',
create_date  String     COMMENT '一级类目id ',
qun_text  String     COMMENT '一级类目id ',
find_schools String
)
COMMENT 'qq 群数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;