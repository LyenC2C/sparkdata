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


create table ppzs_brandid_weeksold(
brand_id string,
weeksold string
)
COMMENT '品牌指数项目品牌周销量表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;


create TABLE wlservice.ppzs_brandid_feed(
brand_id string,
feed_id string,
item_id string,
user_id string,
rate_type string,
content string
)
COMMENT '品牌指数项目品牌评论表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

create TABLE wlservice.ppzs_brandid_rate_count(
brand_id string,
good_count string,
mid_count string,
bad_count string
)
COMMENT '品牌指数项目品牌好中差评数量'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

create table wlservice.ppzs_brandid_top6item
(
brand_id string,
item_id string,
title string,
price string,
picurl string,
now_sold string,
rn string
)
COMMENT '品牌指数项目品牌销量top6商品'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;