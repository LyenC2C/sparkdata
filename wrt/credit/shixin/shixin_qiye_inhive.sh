#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/shixin/'
now_day=$1
last_day=$2
hfs -rmr /user/wrt/temp/shixin_personinfo
spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 $dev_path/shixin_qiye.py $now_day

hive<<EOF

use wlcredit;

LOAD DATA INPATH '/user/wrt/temp/shixin_qiyeinfo' OVERWRITE INTO TABLE t_wrt_shixin_qiye PARTITION (ds='temp');


insert overwrite table t_wrt_shixin_qiye partition (ds = $now_day)
select
t1.id,
t1.iname,
t1.casecode,
t1.cardnum,
t1.businessentity,
t1.courtname,
t1.areaname,
t1.partytypename,
t1.gistid,
t1.regdate,
t1.gistunit,
t1.duty,
t1.performance,
t1.disrupttypename,
t1.publishdate,
t1.performedpart,
t1.unperformpart
from
(select * from t_wrt_shixin_qiye where ds = 'temp')t1
left join
(select * from t_wrt_shixin_person where ds = $last_day)t2
on
t1.id = t2.id
where
t2.id is null;

EOF