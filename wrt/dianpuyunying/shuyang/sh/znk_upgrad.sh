#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/dianpuyunying/shuyang'
save_path='/mnt/raid1/wrt/dianpuyunying/shuyang/development'
now_day=$1
last_day=$2
sold_day=$3

hfs -rmr /user/wrt/temp/znk_iteminfo_tmp
spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80  \
$dev_path/t_wrt_znk_iteminfo.py $now_day $last_day

hfs -rmr /user/wrt/temp/znk_feedmark_tmp
spark-submit  --executor-memory 1G  --driver-memory 5G  --total-executor-cores 80 \
$dev_path/t_wrt_znk_feedmark.py $now_day

hive<<EOF

use wlservice;
LOAD DATA  INPATH '/user/wrt/temp/znk_iteminfo_tmp' OVERWRITE INTO TABLE t_wrt_znk_iteminfo_new PARTITION (ds='$now_day');
LOAD DATA  INPATH '/user/wrt/temp/znk_feedmark_tmp' OVERWRITE INTO TABLE t_wrt_znk_feedmark;

insert overwrite table t_wrt_znk_record partition(ds = '$now_day')
select
case when tt2.item_id is null then tt1.item_id else tt2.item_id end,
case when tt2.feed_id is null then tt1.feed_id else tt2.feed_id end,
case when tt2.user_id is null then tt1.user_id else tt2.user_id end,
case when tt2.dsn is null then tt1.dsn else tt2.dsn end
from
(
select
t1.item_id,
t1.feed_id,
t2.id1 as user_id,
t1.dsn
from
t_wrt_znk_feedmark t1
join
(select id1,uid from wlbase_dev.t_base_uid_tmp where ds = 'uid_mark') t2
ON
t1.usermark = t2.uid
)tt1
full JOIN
(select * from t_wrt_znk_record where ds = '$last_day')tt2
ON
tt1.feed_id = tt2.feed_id;

insert overwrite table t_wrt_znk_development_data partition(ds = '$now_day')
SELECT
tt2.feed_id,
tt2.user_id,
tt2.item_id,
tt2.dsn,
tt1.title,
tt1.brand_name,
tt1.item_size,
tt1.item_type,
tt1.item_count,
tt1.price,
tt1.pic_url,
tt1.sold
FROM
(
select t1.*,
case when t2.item_id is null then "-" else t2.total end as sold
FROM
(select * from t_wrt_znk_iteminfo_new where ds = '$now_day')t1
left JOIN
(select item_id,total from wlbase_dev.t_base_ec_item_sold_dev where ds = '$sold_day')t2
ON
t1.item_id = t2.item_id
)tt1
JOIN
(
select * from t_wrt_znk_record where ds = '$now_day'
)tt2
ON
tt1.item_id = tt2.item_id;

insert overwrite table t_wrt_znk_userid
select user_id from t_wrt_znk_development_data where ds = '$now_day' group by user_id;
EOF

sh $dev_path/t_wrt_znk_othercat.sql

hfs -cat /hive/warehouse/wlservice.db/t_wrt_znk_development_data/ds=$now_day/* > $save_path/znk_development_$now_day

hfs -cat /hive/warehouse/wlservice.db/t_wrt_znk_othercat/* > $save_path/znk_othercat_$now_day

scp $save_path/znk_development_$now_day hadoop@192.168.4.251:/media/disk1/

scp $save_path/znk_othercat_$now_day hadoop@192.168.4.251:/media/disk1/
