CREATE EXTERNAL TABLE  if not exists t_base_ec_item_feed_inc (
item_id STRING  COMMENT  '',
inc  bigint  COMMENT  '������'
)
COMMENT '������Ʒ�û����۱�'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;

