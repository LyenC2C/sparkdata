#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/shixin/'
now_day=$(date -d '0 days ago' +%Y%m%d)

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
use wl_base;
set mapred.max.split.size=268435456;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.min.split.size.per.node=201326592;
set mapred.min.split.size.per.rack=201326592;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=201326592;
set hive.exec.reducers.bytes.per.reducer=268435456;
set hive.exec.reducers.max=1099;
set mapreduce.job.reduces=-1;
LOAD DATA INPATH '/user/wrt/temp/shixin_qiyeinfo' OVERWRITE INTO TABLE t_wrt_shixin_qiye PARTITION (ds='0temp');
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
(select * from t_wrt_shixin_qiye where ds = '0temp')t1
left join
(select * from t_wrt_shixin_qiye where ds <> '0temp')t2
on
t1.id = t2.id
where
t2.id is null;
EOF