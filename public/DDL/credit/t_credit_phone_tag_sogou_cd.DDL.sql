CREATE  TABLE  if not exists wlcredit.t_credit_phone_tag_sogou_cd (
phone string comment '电话',
platform string comment '来自平台',
source string comment '来源',
amount string comment '被标记数量',
tag string comment '标签',
place string comment '地区',
tel_co string comment '电话所属运营商',
ts string comment '时间戳'
)
COMMENT '四川搜狗电话黑名单';
