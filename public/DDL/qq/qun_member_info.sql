CREATE  TABLE  if not exists qun_member_info (
qq_id  String     COMMENT 'һ����Ŀid ',
field1  String     COMMENT 'һ����Ŀid ',
field2  String     COMMENT 'һ����Ŀid ',
field3  String     COMMENT 'һ����Ŀid ',
qun_id  String     COMMENT 'һ����Ŀid '
)
COMMENT 'qq Ⱥ�û�����'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;