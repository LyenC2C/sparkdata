CREATE EXTERNAL TABLE  if not exists t_base_ec_item_feed_dev_zlj (
item_id STRING  COMMENT  '商品id',
item_title STRING  COMMENT  '商品title',
feed_id STRING COMMENT '评论id',
user_id STRING COMMENT '用户id',
content STRING COMMENT '评论内容',
f_date STRING COMMENT '评论日期',
annoy STRING COMMENT '是否匿名',
ts STRING COMMENT '采集时间戳'
)
COMMENT '电商商品用户评价表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;
-- stored as textfile location '/hive/external/wlbase_dev/t_base_ec_item_feed_dev/';


CREATE EXTERNAL TABLE  if not exists t_base_ec_item_feed_dev (
item_id STRING  COMMENT  '商品id',
item_title STRING  COMMENT  '商品title',
feed_id STRING COMMENT '评论id',
user_id STRING COMMENT '用户id',
content STRING COMMENT '评论内容',
f_date STRING COMMENT '评论日期',
annoy STRING COMMENT '是否匿名',
ts STRING COMMENT '采集时间戳'
)
COMMENT '电商商品用户评价表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;