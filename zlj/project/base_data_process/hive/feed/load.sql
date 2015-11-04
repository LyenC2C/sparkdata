SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;

use wlbase_dev;

LOAD DATA  INPATH '/data/develop/ec/tb/cmt_res_tmp/res2013/cmt2013' OVERWRITE INTO TABLE default.t_base_ec_item_feed_dev_zlj PARTITION (ds='20150000');
-- LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev_test/ds=20150000' OVERWRITE INTO TABLE t_base_ec_item_feed_dev_zlj PARTITION (ds='20150000');


INSERT overwrite  TABLE t_base_ec_item_feed_dev PARTITION (ds )

select
item_id,

feed_id,
user_id,
content,
f_date ,
annoy  ,
ts ,
regexp_replace(f_date,'-','') ds
from
default.t_base_ec_item_feed_dev_zlj
where
ds='20150000' and LENGTH(f_date)<12
;


-- select *
-- from
-- t_base_ec_item_feed_dev_test
-- where
-- ds='20150000' and  LENGTH(f_date)>12;