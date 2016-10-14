CREATE  TABLE  if not exists qun_member_info (
qq_id  String     COMMENT 'qq id',
field1  String     COMMENT '年龄 ',
field2  String     COMMENT '性别  0男 1女',
field3  String     COMMENT 'һ����Ŀid ',
qun_id  String     COMMENT 'һ����Ŀid ',
qqname  String     COMMENT 'һ����Ŀid '
)
COMMENT 'qq Ⱥ�û�����'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;

ALTER  TABLE    qun_member_info SET TBLPROPERTIES ('EXTERNAL'='TRUE');

ALTER  TABLE  qun_member_info ADD COLUMNS (qqname STRING);


ALTER TABLE  qun_member_info   drop    PARTITION (ds='info');




LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/qun_member_info/ds=info' OVERWRITE INTO TABLE qq_group_info PARTITION (ds='info');

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/qq_group_info/ds=info' OVERWRITE INTO TABLE qun_member_info PARTITION (ds='info');