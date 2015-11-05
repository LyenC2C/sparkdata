CREATE EXTERNAL TABLE  if not exists t_base_ec_item_feed_inc (
item_id STRING  COMMENT  '',
inc  bigint  COMMENT  '增量数'
)
COMMENT '电商商品用户评价表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;

