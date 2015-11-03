

use wlbase_dev;

-- drop table if EXISTS  t_zlj_ec_maxfeedId;

-- CREATE  table  t_zlj_ec_maxfeedId as

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=256000000;
set mapred.min.split.size.per.rack=256000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;


insert  OVERWRITE table t_base_ec_feed_add_everyday PARTITION(ds=20151101)
SELECT
item_id,max(CAST(feed_id as bigint )) feedid,count(1) as feed_times

FROM
t_base_ec_item_feed_dev
where LENGTH(item_id)>5
group by item_id
;

-- -- SELECT
-- --  item_id,feed_id,f_date,rn
-- -- FROM
-- -- (
-- SELECT
-- item_id,feed_id,f_date,
-- -- row_number()  OVER (PARTITION BY item_id ORDER BY CAST (feed_id as bigint) desc) as rn
-- FROM
-- t_base_ec_item_feed_dev
-- group by item_id
-- -- WHERE ds
-- -- )t where rn=1
