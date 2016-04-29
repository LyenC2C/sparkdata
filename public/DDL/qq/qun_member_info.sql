CREATE  TABLE  if not exists qun_member_info (
qq_id  String     COMMENT '一级类目id ',
field1  String     COMMENT '一级类目id ',
field2  String     COMMENT '一级类目id ',
field3  String     COMMENT '一级类目id ',
qun_id  String     COMMENT '一级类目id '
)
COMMENT 'qq 群用户数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;