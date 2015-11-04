SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;
set mapred.max.split.size=100000000;
use wlbase_dev;

-- LOAD DATA  INPATH '/user/zlj/data/res2015_re' OVERWRITE INTO TABLE t_base_ec_item_feed_dev PARTITION (ds='20100000');
-- LOAD DATA  INPATH '/user/zlj/data/res2014_re' OVERWRITE INTO TABLE t_base_ec_item_feed_dev_zlj PARTITION (ds='20100000');
LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev/ds=20100000' OVERWRITE INTO TABLE t_base_ec_item_feed_dev_zlj PARTITION (ds='20100005');



INSERT overwrite  TABLE t_base_ec_item_feed_dev PARTITION (ds )
select
item_id,
item_title,
feed_id,
user_id,
content,
f_date ,
annoy  ,
ts ,
regexp_replace(f_date,'-','') ds
from
t_base_ec_item_feed_dev_zlj
where
ds='20100005' and LENGTH(f_date)>0
;


-- select *
-- from
-- t_base_ec_item_feed_dev_test
-- where
-- ds='20150000' and  LENGTH(f_date)>12;