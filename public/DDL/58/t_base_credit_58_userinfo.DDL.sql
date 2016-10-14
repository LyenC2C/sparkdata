CREATE  TABLE  if not exists t_base_credit_58_userinfo (
decrypted_tel      string   comment '手机号',
nickname      string   comment '用户昵称',
uid      string   comment '用户id',
uname         string   comment '来源名称-猜测应该是员工工号之类'
)
COMMENT '58用户基本信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;


