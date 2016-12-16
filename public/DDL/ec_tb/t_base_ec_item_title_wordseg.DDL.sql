
CREATE EXTERNAL TABLE  if not exists wlbase_dev.t_base_ec_item_title_wordseg (
item_id STRING  COMMENT  '商品id',
title_seg  STRING   COMMENT '商品title分词',
title_seg_clean string COMMENT '商品title分词过滤清理标点等'
)
COMMENT '电商商品title 阿里分词表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   ;


  LOAD DATA     INPATH '/user/lel/re' OVERWRITE
  INTO TABLE wlbase_dev.t_base_ec_item_title_wordseg PARTITION (ds='20161212') ;


create table t_base_ec_item_title_wordseg_user_1212 as
SELECT
t1.user_id ,t2.title_seg_clean
FROM
(
  SELECT user_id ,item_id from t_base_ec_item_feed_dev_new
  group by
   user_id ,item_id
)t1 join t_base_ec_item_title_wordseg t2 on t1.item_id=t2.item_id ;


create table t_base_ec_item_title_wordseg_user_1212_group as

select user_id ,concat_ws('\003', collect_set(title_seg_clean)) AS brandtags

 from t_base_ec_item_title_wordseg_user_1212
group by user_id,