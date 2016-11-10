create table wlservice.ppzs_itemid_info(
item_id string,
brand_id string,
title string,
picurl string,
price string
)
COMMENT '品牌指数项目商品信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;
--