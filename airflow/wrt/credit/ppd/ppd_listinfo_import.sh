#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/ppd/'
now_day=$(date -d '0 days ago' +%Y%m%d)

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
use wl_base;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=201326592;
set hive.exec.reducers.bytes.per.reducer=268435456;
set hive.support.quoted.identifiers=none;
LOAD DATA INPATH '/user/wrt/temp/ppd_info_tmp' OVERWRITE INTO TABLE wl_base.t_base_credit_ppd_listinfo PARTITION (ds=$now_day);
insert OVERWRITE table wl_base.t_base_credit_ppd_listinfo PARTITION(ds = $now_day)
select \`(ds)?+.+\` from wl_base.t_base_credit_ppd_listinfo where ds = $now_day;
EOF
