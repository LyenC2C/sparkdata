#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
lastday=$(date -d '7 days ago' +%Y%m%d)

table=wl_base.t_base_ec_item_dev_new

hive<<EOF

use wl_base;
LOAD DATA  INPATH '/user/wrt/temp/iteminfo_tmp' OVERWRITE INTO TABLE $table PARTITION(ds='0temp');

insert OVERWRITE table $table PARTITION(ds = $today)
select
case when t1.item_id is null then t2.item_id else t1.item_id end,
case when t1.item_id is null then t2.title else t1.title end,
case when t1.item_id is null then t2.cat_id else t1.cat_id end,
case when t1.item_id is null then t2.cat_ name else t1.cat_name end,
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
(select * from $table where ds = '0temp')t1
full outer JOIN
(select * from $table where ds = $lastday)t2
ON
t1.item_id = t2.item_id;
EOF
