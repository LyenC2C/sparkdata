
#!/bin/bash

source /etc/profile;

m=1
m_1=`expr $m + 1`
d_1=$(date -d '-'$m' day' '+%Y%m%d')
d_2=$(date -d '-'$m_1' day' '+%Y%m%d')


-- d_1=$(date -d '-1 day' '+%Y-%m-%d')
-- d_2=$(date -d '-2 day' '+%Y-%m-%d')

lastmonth=$(date -d '-1 month' '+%Y-%m-%d')
path=$2



##hive  部分

/home/zlj/hive/bin/hive<<EOF


SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;

use wlbase_dev;

LOAD DATA  INPATH '$path' OVERWRITE INTO TABLE t_base_ec_item_feed_dev_zlj PARTITION (ds='20101103');



-- 动态分区
-- INSERT overwrite TABLE t_base_ec_item_feed_dev PARTITION (ds )

INSERT INTO TABLE t_base_ec_item_feed_dev PARTITION (ds )
select
t3.item_id,
t3.item_title,
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
  when t1.item_id is not NULL and t1.maxfeed_id<t2.feedid then 1
  else 0 end  as rn
  from
  (
    SELECT
    *
    from
    t_base_ec_feed_add_everyday
--     where ds=cast(from_unixtime(unix_timestamp()-86400*3,'yyyyMMdd') as String)
    where ds='$d_2'

  )t1
  RIGHT  OUTER  join
  (
    SELECT
    item_id,
    item_title,
    CAST (feed_id as bigint) feedid,
    user_id,
    content,
    f_date ,
    annoy  ,
    ts ,
    regexp_replace(f_date,'-','') ds
    FROM t_base_ec_item_feed_dev_zlj where ds=20101103  and item_id rlike   '^\\d+$'

  )t2 on t1.item_id=t2.item_id
)t3 where rn=1 ;




-- 最大分区
insert overwrite  table t_base_ec_feed_add_everyday PARTITION(ds)
  select
  item_id,max(maxfeed_id) as feedid ,sum(feed_times)
-- ,cast(from_unixtime(unix_timestamp()-86400*2,'yyyyMMdd') as STRING) ds
,'$d_1' ds
  from
(
   SELECT
    item_id,
    maxfeed_id,
    feed_times
    from
    t_base_ec_feed_add_everyday
--        where ds=cast(from_unixtime(unix_timestamp()-86400*3,'yyyyMMdd') as STRING)
     where ds='$d_2' and item_id rlike   '^\\d+$'
    UNION  ALL
     SELECT
    item_id,
    max(CAST (feed_id as bigint)) maxfeed_id ,
    count(1) as feed_times

    FROM t_base_ec_item_feed_dev_zlj where ds=20101103 and item_id rlike   '^\\d+$'
    group by item_id

)t group by item_id  ;


EOF

-- insert overwrite  table t_base_ec_feed_add_everyday PARTITION(ds='20151102')
-- select item_id,max(CAST (feed_id as bigint))  maxfeed_id, count(1) as feed_times from t_base_ec_item_feed_dev
-- where item_id  rlike   '^\\d+$'
-- group by item_id;
