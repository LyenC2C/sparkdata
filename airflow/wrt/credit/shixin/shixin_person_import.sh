#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/shixin/'
now_day=$(date -d '0 days ago' +%Y%m%d)

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
use wl_base;
LOAD DATA INPATH '/user/wrt/temp/shixin_personinfo' OVERWRITE INTO TABLE t_wrt_shixin_person PARTITION (ds='0temp');
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
(select * from t_wrt_shixin_person where ds = '0temp')t1
left join
(select * from t_wrt_shixin_person where ds <> '0temp' )t2
on
t1.id = t2.id
where
t2.id is null;
EOF
