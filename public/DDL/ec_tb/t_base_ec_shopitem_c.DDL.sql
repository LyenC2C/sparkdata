CREATE TABLE t_base_ec_shopitem_c (
shop_id String COMMENT '店铺id',
item_id String COMMENT '商品id',
sold BIGINT COMMENT '销量',
salePrice float COMMENT '商品现价',
up_day String COMMENT '上架日期',
down_day String COMMENT '下架日期（未下架用0）',
ts String COMMENT '采集时间戳'
)
COMMENT '淘宝店铺的商品列表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' ;