CREATE  TABLE  if not exists t_base_credit_58_info (
t_action        string   comment '未知',
cateid        string   comment '类目id',
catename      string   comment '类目名称',
decrypted_tel      string   comment '手机号',
infoid        string   comment '发帖id',
isbiz         string   comment '未知',
nickname      string   comment '用户昵称',
online        string   comment '是否在线',
rootcateid         string   comment '根类目',
title         string   comment '帖子title',
tradeline          string   comment '交易线',
uid      string   comment '用户id',
uname         string   comment '来源名称-猜测应该是员工工号之类'
)
COMMENT '58发帖数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;

