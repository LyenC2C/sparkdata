#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
last_update=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_record_data_backflow | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
set hive.support.quoted.identifiers=none;
load data inpath '/user/lel/temp/record_data_backflow' overwrite into table wl_base.t_base_record_data_backflow partition(ds='00tmp');
insert overwrite table wl_base.t_base_record_data_backflow partition(ds=$today)
select distinct * from
(select \`(ds)?+.+\` from wl_base.t_base_record_data_backflow where ds='00tmp'
union all
select \`(ds)?+.+\` from wl_base.t_base_record_data_backflow where ds=$last_update)a;
EOF