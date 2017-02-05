#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
now_day=$1
last_day=$2

haddoop fs  -rmr /user/wrt/shopitem_c_tmp

spark-submit  --executor-memory 6G  --driver-memory 5G  --total-executor-cores 80 \
$pre_path/wrt/data_base_process/t_base_shopitem_c.py $now_day >> \
$pre_path/wrt/data_base_process/sh/log_shopitem/log_c_$now_day 2>&1

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/wrt/shopitem_c_tmp' OVERWRITE INTO TABLE t_base_ec_shopitem_c PARTITION (ds='0temp');

insert OVERWRITE table t_base_ec_shopitem_c PARTITION(ds = $now_day)
select
case when t1.item_id is null then t2.shop_id else t1.shop_id end,
case when t1.item_id is null then t2.item_id else t1.item_id end,
case when t1.item_id is null then t2.sold else t1.sold end,
case when t1.item_id is null then t2.saleprice else t1.saleprice  end,
case when t2.item_id is null then t1.up_day else t2.up_day end,
case when t1.item_id is null then t2.update_day else t1.update_day end,
case when t1.item_id is null then t2.ts else t1.ts end
from
(select * from t_base_ec_shopitem_c where ds = '0temp')t1
full outer join
(select * from t_base_ec_shopitem_c where ds = $last_day)t2
on
t1.item_id = t2.item_id;
EOF
