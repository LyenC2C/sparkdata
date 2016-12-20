CREATE TABLE  if not exists wlbase_dev.t_base_ec_xianyu_iteminfo (
id String COMMENT '商品id',
userId String COMMENT '用户id',
title String COMMENT '商品所属标签',
province String COMMENT '省份',
city String COMMENT '城市',
area String COMMENT '区域',
auctionType String COMMENT '拍卖类型',
description String COMMENT '商品描述',
detailFrom String COMMENT '终端',
favorNum String COMMENT '被赞次数',
commentNum String COMMENT '评论次数',
firstModified String COMMENT '第一次更改时间',
firstModifiedDiff String COMMENT '第一次更改时间间隔',
t_from String COMMENT '终端',
gps String COMMENT '经纬度',
offline String COMMENT '是否下线',
originalPrice String COMMENT '原价',
price String COMMENT '价格',
userNick String COMMENT '用户别名',
categoryId String COMMENT '商品所属类别id',
categoryName String COMMENT '商品所属类别名',
fishPoolId String COMMENT '鱼塘id',
fishpoolName String COMMENT '鱼塘名',
bar String COMMENT '类似鱼塘名',
barInfo String COMMENT 'bar等级',
abbr String COMMENT '用户标签',
zhima String COMMENT '芝麻信用认证',
shiren String COMMENT '实人认证',
ts String COMMENT '时间戳'
)
COMMENT '闲鱼商品信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;
