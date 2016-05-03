CREATE EXTERNAL TABLE  if not exists t_base_ec_item_sold_dev (
item_id string comment '商品id',
price float comment '价格',
amount bigint comment '月销量',
total bigint comment '总销量',
qu bigint comment '库存',
st string comment '上下架等级',
inSale string comment '是否上架',
start string comment '记录时间戳',
cp_flag string comment '复制flag',
ts string comment '采集时间戳'
)
COMMENT '电商商品销量状态表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' ;



