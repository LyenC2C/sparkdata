CREATE  TABLE  if not exists t_zlj_ec_item_feed_count (
item_id   String COMMENT '��Ʒid',
feedids  String
)
COMMENT '��Ʒ����10��feed id'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;