#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
today=$1

hfs -rmr /user/wrt/temp/iteminfo_tmp
spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_info.py -spark

hive<<EOF

use wl_base;
LOAD DATA  INPATH '/user/wrt/temp/iteminfo_tmp' OVERWRITE INTO TABLE t_base_ec_item_dev_new PARTITION (ds=$today);

insert into table t_base_ec_item_dev_new PARTITION(ds=$today)
select
t1.item_id,
t1.title,
t1.cat_id,
t1.cat_name,
t1.root_cat_id ,
t1.root_cat_name,
t1.brand_id,
t1.brand_name,
t1.bc_type,
t1.price,
t1.price_zone,
"2",
"-1",
t1.favor,
t1.seller_id,
t1.shop_id,
t1.location,
t1.paramap,
t1.paramap,
t1.ts
from
(select * from t_base_ec_item_dev where ds = 20160333)t1
left JOIN
(select * from t_base_ec_item_dev_new where ds=$today)t2
ON
t1.item_id = t2.item_id
where
t2.item_id is null;

EOF

hfs -rmr /user/wrt/temp/shopinfo_tmp
spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_shop_info.py
hive<<EOF
use wl_base;
LOAD DATA  INPATH '/user/wrt/temp/shopinfo_tmp' OVERWRITE INTO TABLE t_base_ec_shop_dev_new PARTITION (ds=$today);
EOF