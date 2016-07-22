CREATE EXTERNAL TABLE  if not exists t_base_ec_shopitem (
shop_id String COMMENT '店铺id',
shopTitle String COMMENT '店铺名字',
item_count String COMMENT '店铺商品总数',
auctionId String COMMENT '商品id',
title String COMMENT '商品title',
picUrl String COMMENT '商品图片url',
sold BIGINT COMMENT '销量',
reservePrice float COMMENT '商品原价',
salePrice float COMMENT '商品现价',
auctionType String COMMENT '商品类型（非淘宝天猫）',
quantity String BIGINT '月库存？',
totalSoldQuantity BIGINT COMMENT '总库存？',
orderCost String COMMENT '？',
bonusAmount String COMMENT '？',
onSale String COMMENT '？',
up_day String COMMENT '上架日期',
down_day String COMMENT '下架日期（未下架用0）',
ts String COMMENT '采集时间戳'
)
COMMENT '电商商品用户销量状态表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' ;