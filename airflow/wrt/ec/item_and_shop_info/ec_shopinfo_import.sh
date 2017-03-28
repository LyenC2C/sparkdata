#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
last=$1

table=wl_base.t_base_ec_shop_dev_new
database=wl_base
db_path=$database.db
estimate_date=`hadoop fs -ls /hive/warehouse/$db_path/$table | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
total_size=`hadoop fs -du -s  /hive/warehouse/$db_path/$table/ds=$estimate_date | awk '{print $1/1024/1024}'`
offset=5
dynamic_reducers=`awk 'BEGIN{print int(('$total_size'/256)+0.5)+3}'`

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
use $database;
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
set mapreduce.job.reduces=$dynamic_reducers;
LOAD DATA  INPATH "/user/wrt/temp/shopinfo_tmp" OVERWRITE INTO TABLE $table PARTITION(ds='00tmp');
insert OVERWRITE table $table PARTITION(ds = $today)
select
case when t1.shop_id is null then t2.shop_id else t1.shop_id end,
case when t1.shop_id is null then t2.seller_id else t1.seller_id end,
case when t1.shop_id is null then t2.shop_name else t1.shop_name end,
case when t1.shop_id is null then t2.seller_name else t1.seller_name end,
case when t1.shop_id is null then t2.star else t1.star end,
case when t1.shop_id is null then t2.credit else t1.credit end,
case when t1.shop_id is null then t2.starts else t1.starts end,
case when t1.shop_id is null then t2.bc_type else t1.bc_type end,
case when t1.shop_id is null then t2.item_count else t1.item_count end,
case when t1.shop_id is null then t2.fans_count else t1.fans_count end,
case when t1.shop_id is null then t2.good_rate_p else t1.good_rate_p end,
case when t1.shop_id is null then t2.weitao_id else t1.weitao_id end,
case when t1.shop_id is null then t2.desc_score else t1.desc_score end,
case when t1.shop_id is null then t2.service_score else t1.service_score end,
case when t1.shop_id is null then t2.wuliu_score else t1.wuliu_score end,
case when t1.shop_id is null then t2.location else t1.location end,
case when t1.shop_id is null then t2.ts else t1.ts end,
case when t1.shop_id is null then t2.desc_highgap else t1.desc_highgap end,
case when t1.shop_id is null then t2.service_highgap else t1.service_highgap end,
case when t1.shop_id is null then t2.wuliu_highgap else t1.wuliu_highgap end,
case when t1.shop_id is null then t2.is_online else t1.is_online end,
case when t1.shop_id is null then t2.shop_type else t1.shop_type end,
case when t1.shop_id is null then t2.shop_certifi else t1.shop_certifi end
from
(select * from $table where ds = "00tmp")t1
full outer JOIN
(select * from $table where ds = $last)t2
ON
t1.shop_id = t2.shop_id;
EOF

