use wlbase_dev;

CREATE EXTERNAL TABLE  if not exists t_base_ec_item_sale_neo(
item_id	 STRING  COMMENT  '商品id',
r_price    float COMMENT '原价',
s_price   float COMMENT '售价',
quantity  BIGINT	COMMENT '库存',
total_sold  BIGINT COMMENT '总销量',
shop_id	  STRING COMMENT '店铺id',
ts	  STRING COMMENT '采集时间戳'
);