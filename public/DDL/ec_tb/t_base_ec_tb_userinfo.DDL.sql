CREATE EXTERNAL TABLE  if not exists t_base_ec_tb_userinfo (
uid string comment 'userid',
alipay string comment '是否支付宝实名 1为是，0为否',
buycnt string comment '购买记录',
verify string comment '淘宝等级',
regtime string comment '注册时间',
nick string comment '昵称',
location string comment '用户地点'
)
COMMENT '电商淘宝用户信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;