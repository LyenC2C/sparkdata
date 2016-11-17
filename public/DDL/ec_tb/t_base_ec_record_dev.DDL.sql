-- CREATE EXTERNAL TABLE  if not exists wlbase_dev.t_base_ec_record_dev (
-- item_id STRING ,
-- feed_id STRING ,
-- user_id STRING ,
-- content_length BIGINT ,
-- annoy BIGINT ,
-- ds STRING ,
-- datediff BIGINT,
-- title STRING,
-- cat_id STRING ,
-- root_cat_id STRING,
-- root_cat_name  STRING ,
-- brand_id STRING ,
-- brand_name  STRING,
-- bc_type STRING,
-- price double  ,
-- location STRING
-- )
-- COMMENT '评价 商品详情join表'
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
-- stored as textfile location '/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev/';


CREATE EXTERNAL TABLE  if not exists wlbase_dev.t_base_ec_record_dev_new_t (
item_id                 bigint    COMMENT '商品id' ,
feed_id                 string    COMMENT '评论id' ,
user_id                 string    COMMENT '用户id' ,
content_length          int       COMMENT '评论长度' ,
annoy                   string    COMMENT '是否匿名' ,
dsn                     string    COMMENT '评论日期' ,
datediff                int       COMMENT '时间差' ,
date_predict            string    COMMENT '根据评论分析的购买时间' ,
sku                     string    COMMENT 'sku' ,
title                   string    COMMENT '商品title' ,
cat_id                  string    COMMENT '叶子类目id' ,
root_cat_id             string    COMMENT '一级类目id' ,
root_cat_name           string    COMMENT '一类类目名称' ,
brand_id                string    COMMENT '品牌id' ,
brand_name              string    COMMENT '品牌名字' ,
bc_type                 string    COMMENT 'b天猫c集市' ,
price                   string    COMMENT '价格' ,
shop_id                 string    COMMENT '店铺id' ,
location                string    COMMENT '店铺地址' ,
tel_index               string    COMMENT '手机号编码' ,
tel_user_rn             int        COMMENT '手机号对应淘宝id次数编号'
)
COMMENT '用户购买记录评价 商品详情join表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile location '/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev/';