CREATE  TABLE  if not exists wlcredit.t_credit_phone_tag_sogou (
phone string comment '电话',
platform string comment '来自平台',
source string comment '来源',
amount string comment '被标记数量',
tag string comment '标签',
place string comment '地区',
tel_co string comment '电话所属运营商',
ts string comment '时间戳'
)
COMMENT '全国搜狗电话黑名单'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;