

CREATE EXTERNAL TABLE  if not exists t_base_ec_tb_xianyu_userinfo (
userId  string comment '' ,
totalCount  string comment '',
gender  string comment '性别',
idleUserId  string comment '账号是否闲置 猜测',
nick  string comment '闲鱼昵称',
tradeCount  string comment '',
tradeIncome  string comment '',
userNick  string comment '淘宝昵称',
constellation  string comment '星座',
birthday  string comment '出生年月',

city  string comment '城市'
)
COMMENT '电商淘宝闲鱼用户信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;