CREATE  TABLE  if not exists t_base_qqqun_unserinfo (
qq string comment 'qq号',
age string comment '年龄',
sex string comment '性别',
realname string comment '真名'
)
COMMENT 'qq群成员的个人信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;