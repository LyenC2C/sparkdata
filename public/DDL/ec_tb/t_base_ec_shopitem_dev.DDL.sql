CREATE EXTERNAL TABLE  if not exists t_base_ec_shopitem_dev (
shop_id String COMMENT '店铺id',
shopTitle String COMMENT '店铺名字',
item_count String COMMENT '店铺商品总数',
auctionId String COMMENT '商品id',
title String COMMENT '商品title',
picUrl String COMMENT '商品图片url',
sold BIGINT COMMENT '销量',
day_sold BIGINT COMMET '日销量',
reservePrice float COMMENT '商品原价',
salePrice float COMMENT '商品现价',
auctionType String COMMENT '商品类型（非淘宝天猫）',
quantity BIGINT COMMENT '月库存？',
totalSoldQuantity BIGINT COMMENT '总库存？',
orderCost String COMMENT '？',
bonusAmount String COMMENT '？',
onSale String COMMENT '？',
up_day String COMMENT '上架日期',
down_day String COMMENT '下架日期（未下架用0）',
ts String COMMENT '采集时间戳'
)
COMMENT '店铺的商品列表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' ;

-- 此表用于：
-- 1.给采集组提供最新的item_id,其中item_id可以按照销量排序
-- 2.给开发组提供“店铺运营”项目数据