
CREATE  TABLE  if not exists t_base_weibo_user_basic (
id   string  COMMENT '微博id',
birthday   string  COMMENT '出生年月',
birthday_visible   string  COMMENT '出生年月可见等级',
city   string  COMMENT '城市编码',
completion_level   string  COMMENT '',
created_at   string  COMMENT '注册时间',
credentials_num   string  COMMENT '',
credentials_type   string  COMMENT '',
default_avatar   string  COMMENT '默认头像',
description   string  COMMENT '描述',
domain   string  COMMENT '',
email   string  COMMENT '邮箱',
email_visible   string  COMMENT '邮箱可见等级',
gender   string  COMMENT '性别',
lang   string  COMMENT '语言',
location   string  COMMENT '地址',
msn   string  COMMENT '',
msn_visible   string  COMMENT '',
name   string  COMMENT '昵称',
profileImageUrl   string  COMMENT '个人简介头像',
province   string  COMMENT '省',
qq   string  COMMENT 'qq',
qq_visible   string  COMMENT 'qq可见等级',
real_name   string  COMMENT '真实姓名',
real_name_visible   string  COMMENT '姓名是否可见',
screen_name   string  COMMENT '昵称',
url   string  COMMENT 'url',
url_visible string  COMMENT 'url可见等级'
)
COMMENT '微博用户基本信息 注意与t_base_weibo_user表区别'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

LOAD DATA   INPATH '/user/zlj/tmp/user_basic' OVERWRITE INTO TABLE t_base_weibo_user_basic PARTITION (ds='20161018') ;


