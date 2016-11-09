use wlbase_dev;
create table t_base_shop_type
(
shop_id string COMMENT '店铺id',
shop_type map<string, string>   COMMENT '店铺类型'
)
COMMENT '店铺类型表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   COLLECTION ITEMS TERMINATED BY ','   MAP KEYS TERMINATED BY ':'
;
