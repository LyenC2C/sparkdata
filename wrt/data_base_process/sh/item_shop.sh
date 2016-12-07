#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
today=$1
lastday=$2

hfs -rmr /user/wrt/temp/iteminfo_tmp
spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_info.py -spark

hive<<EOF

use wl_base;
LOAD DATA  INPATH '/user/wrt/temp/iteminfo_tmp' OVERWRITE INTO TABLE wl_base.t_base_ec_item_dev_new PARTITION (ds='temp');

insert OVERWRITE table t_base_ec_item_dev_new PARTITION(ds = $today)
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
(select * from t_base_ec_item_dev_new where ds = 'temp')t1
full outer JOIN
(select * from t_base_ec_item_dev_new where ds = $lastday)t2
ON
t1.item_id = t2.item_id;

EOF

hfs -rmr /user/wrt/temp/shopinfo_tmp
spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_shop_info.py
hive<<EOF
use wl_base;
LOAD DATA  INPATH '/user/wrt/temp/shopinfo_tmp' OVERWRITE INTO TABLE t_base_ec_shop_dev_new PARTITION (ds='temp');

insert OVERWRITE table t_base_ec_shop_dev_new PARTITION(ds = $today)
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
(select * from t_base_ec_shop_dev_new where ds = 'temp')t1
full outer JOIN
(select * from t_base_ec_shop_dev_new where ds = $lastday)t2
ON
t1.shop_id = t2.shop_id;

EOF