create table wlservice.ppzs_itemid_brandid(
item_id string,
brand_id string
)
COMMENT '品牌指数项目商品与品牌表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;