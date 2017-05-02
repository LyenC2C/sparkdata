#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
last=$1

table=wl_base.t_base_ec_item_abnormal

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
LOAD DATA  INPATH "/user/lel/temp/iteminfo_abnormal" OVERWRITE INTO TABLE $table PARTITION(ds='00tmp');
insert OVERWRITE table $table PARTITION(ds = $today)
select
case when t1.item_id is null then t2.item_id else t1.item_id end,
case when t1.item_id is null then t2.title else t1.title end,
case when t1.item_id is null then t2.cat_id else t1.cat_id end,
case when t1.item_id is null then t2.cat_name else t1.cat_name end,
case when t1.item_id is null then t2.root_cat_id else t1.root_cat_id end,
case when t1.item_id is null then t2.root_cat_name else t1.root_cat_name end,
case when t1.item_id is null then t2.brand_id else t1.brand_id end,
case when t1.item_id is null then t2.brand_name else t1.brand_name end,
case when t1.item_id is null then t2.bc_type else t1.bc_type end,
case when t1.item_id is null then t2.price else t1.price end,
case when t1.item_id is null then t2.price_zone else t1.price_zone end,
case when t1.item_id is null then t2.is_online else t1.is_online end,
case when t1.item_id is null then t2.off_time else t1.off_time end,
case when t1.item_id is null then t2.favor else t1.favor end,
case when t1.item_id is null then t2.seller_id else t1.seller_id end,
case when t1.item_id is null then t2.shop_id else t1.shop_id end,
case when t1.item_id is null then t2.location else t1.location end,
case when t1.item_id is null then t2.paramap else t1.paramap end,
case when t1.item_id is null then t2.sku else t1.sku end,
case when t1.item_id is null then t2.ts else t1.ts end
from
(select * from $table where ds = "00tmp")t1
full outer JOIN
(select * from $table where ds = $last)t2
ON
t1.item_id = t2.item_id;
EOF

