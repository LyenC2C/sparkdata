create table wlcredit.t_base_muti_platform(
phone string comment '电话',
platform string comment '平台',
reg string comment '是否注册，0为未注册，1为注册过'
)
COMMENT '多平台借贷数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n';