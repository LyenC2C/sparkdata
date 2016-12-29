CREATE  TABLE  if not exists qq_group_info (
qun_id  String     COMMENT '群id',
qun_class  String     COMMENT '未知',
mast_qq  String     COMMENT '未知',
title  String     COMMENT '群名称',
create_date  String     COMMENT '创建时间',
qun_text  String     COMMENT '群简介',
find_schools String
)
COMMENT 'qq群信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;