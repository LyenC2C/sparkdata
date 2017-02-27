create table t_base_ec_item_daysale_dev_new (
item_id string comment '商品id',
day_sold bigint comment '日销量',
day_sold_price float comment '日销售额'
)
COMMENT '电商日销量表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' ;