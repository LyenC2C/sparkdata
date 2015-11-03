CREATE  TABLE  if not exists t_base_ec_feed_add_everyday (
item_id  String,
maxfeed_id bigint ,
feed_times  bigint
)
COMMENT '每日评论统计数'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;