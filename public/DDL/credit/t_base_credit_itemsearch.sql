CREATE  TABLE  if not exists t_base_credit_itemsearch (
provcity   string  COMMENT '省市',
discountPrice   string  COMMENT '折扣价',
rankType   string  COMMENT '未知',
clickUrl   string  COMMENT '跳转链接',
rankNum   string  COMMENT '未知',
showRedbag   string  COMMENT '未知',
title   string  COMMENT '商品title',
pictUrl   string  COMMENT '商品图片',
realPostFee   string  COMMENT '折扣',
reservePrice   string  COMMENT '',
userId   string  COMMENT '用户id',
redPacket   string  COMMENT '未知',
nid   string  COMMENT '商品id',
uvsum   string  COMMENT '未知',
userType   string  COMMENT '用户类型',
nick   string  COMMENT '用户昵称',
auctionTag   string  COMMENT '',
hWScale   string  COMMENT '未知',
tagType   string  COMMENT '未知',
itemType   string  COMMENT '商品类型'
)
COMMENT '信用异常词检索淘宝商品列表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

106317

LOAD DATA  INPATH '/user/zlj/tmp/oper_taobao_item/' OVERWRITE INTO TABLE t_base_credit_itemsearch PARTITION (ds='20161009')