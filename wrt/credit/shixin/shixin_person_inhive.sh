#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/shixin/'
now_day=$1
#last_day=$2

hfs -rmr /user/wrt/temp/shixin_personinfo
spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 $dev_path/shixin_person.py $now_day

hive<<EOF

use wlcredit;

LOAD DATA INPATH '/user/wrt/temp/shixin_personinfo' OVERWRITE INTO TABLE t_wrt_shixin_person PARTITION (ds='temp');


insert overwrite table t_wrt_shixin_person partition (ds = $now_day)
select
t1.id,
t1.iname,
t1.casecode,
t1.cardnum,
t1.age,
t1.sexy,
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
(select * from t_wrt_shixin_person where ds = 'temp')t1
left join
(select * from t_wrt_shixin_person where ds = 'past' )t2
on
t1.id = t2.id
where
t2.id is null;

insert into table table t_wrt_shixin_person partition(ds = 'past')
select
id,
iname,
casecode,
cardnum,
age,
sexy,
businessentity,
courtname,
areaname,
partytypename,
gistid,
regdate,
gistunit,
duty,
performance,
disrupttypename,
publishdate,
performedpart,
unperformpart
from t_wrt_shixin_person where ds = $now_day;

EOF
