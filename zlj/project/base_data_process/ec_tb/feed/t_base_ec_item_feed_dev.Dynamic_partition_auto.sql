SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;

use wlbase_dev;

LOAD DATA  INPATH '/hive/external/wlbase_dev/t_base_ec_item_tag_dev/ds=20000103' OVERWRITE INTO TABLE t_base_ec_item_feed_dev_test PARTITION (ds='20150000');


-- -- 每日新增feed量统计
-- INSERT INTO TABLE t_base_ec_feed_add_everyday PARTITION (ds )
--
-- SELECT
--     item_id,
--    count(1)
--     cast(from_unixtime(unix_timestamp()-86400,'yyyyMMdd') as STRING) ds
--     FROM t_base_ec_item_feed_dev_test where ds=20150000
-- group by item_id ;



-- 动态分区
-- INSERT overwrite TABLE t_base_ec_item_feed_dev PARTITION (ds )
INSERT INTO TABLE t_base_ec_item_feed_dev PARTITION (ds )

select
t3.item_id,
t3.feedid as feed_id,
user_id,
content,
f_date ,
annoy  ,
ts ,
regexp_replace(f_date,'-','') ds

from
(
  select
  t2.*
  ,
  case when t1.item_id is NULL  then 1
  when t1.item_id is not NULL and t1.feedid<t2.feedid then 1
  else 0 end  as rn
  from
  (
    SELECT
    *
    from
    t_zlj_ec_maxfeedId


  )t1
  RIGHT  OUTER  join
  (
    SELECT
    item_id,
    CAST (feed_id as bigint) feedid,
    user_id,
    content,
    f_date ,
    annoy  ,
    ts ,
    regexp_replace(f_date,'-','') ds
    FROM t_base_ec_item_feed_dev_test where ds=20150000

  )t2 on t1.item_id=t2.item_id
)t3 where rn=1 ;




-- 最大分区
insert overwrite  table t_base_ec_feed_add_everyday PARTITION(ds)
  select
  item_id,max(feedid) as feedid ,sum(feed_times)
,cast(from_unixtime(unix_timestamp()-86400,'yyyyMMdd') as STRING) ds
  from
(
   SELECT
    *
    from
    t_base_ec_feed_add_everyday
    where ds=cast(from_unixtime(unix_timestamp()-86400*2,'yyyyMMdd') as STRING)

    UNION  ALL
     SELECT
    item_id,
    max(CAST (feed_id as bigint)) feedid ,
    count(1) as feed_times

    FROM t_base_ec_item_feed_dev where ds=20150000
    group by item_id

)t group by item_id  ;