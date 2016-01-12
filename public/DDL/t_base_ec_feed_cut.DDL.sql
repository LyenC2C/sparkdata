CREATE  TABLE  if not exists t_base_ec_feed_cut (
item_id   String COMMENT '',
feed_id   String COMMENT '',
user_id   String COMMENT '',
cut_feed   String COMMENT ''
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;

create table t_zlj_t_base_ec_feed_cut_cat_feed
as
SELECT root_cat_id,cut_feed
from
(SELECT item_id,root_cat_id  from t_base_ec_item_dev where ds=20160107)
t1 join
(select item_id,cut_feed from t_base_ec_feed_cut)t2 on t1.item_id=t2.item_id;
