#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
lastday=$1
last_2_days=$2

#hadoop fs  -rmr /user/wrt/shopitem_c_tmp
#
#spark-submit --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 \
#$pre_path/wrt/data_base_process/t_base_shopitem_c.py $lastday
#
#table=wl_base.t_base_ec_shopitem_c

hive<<EOF
LOAD DATA  INPATH '/user/wrt/shopitem_c_tmp' OVERWRITE INTO TABLE $table PARTITION (ds='0temp');
#insert OVERWRITE table $table PARTITION(ds = $lastday)
#select
#case when t1.item_id is null then t2.shop_id else t1.shop_id end,
#case when t1.item_id is null then t2.item_id else t1.item_id end,
#case when t1.item_id is null then t2.sold else t1.sold end,
#case when t1.item_id is null then t2.saleprice else t1.saleprice  end,
#case when t2.item_id is null then t1.up_day else t2.up_day end,
#case when t1.item_id is null then t2.update_day else t1.update_day end,
#case when t1.item_id is null then t2.ts else t1.ts end
#from
#(select * from $table where ds = '0temp')t1
#full outer join
#(select * from $table where ds = $last_2_days)t2
#on
#t1.item_id = t2.item_id;
EOF

