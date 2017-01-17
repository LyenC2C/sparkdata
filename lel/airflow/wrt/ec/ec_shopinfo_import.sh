#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
lastday=$(date -d '7 days ago' +%Y%m%d)

table=wlbase_dev.t_base_ec_shop_dev_new

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/wrt/temp/shopinfo_tmp' OVERWRITE INTO TABLE $table PARTITION(ds='0temp');

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
(select * from $table where ds = '0temp')t1
full outer JOIN
(select * from $table where ds = $lastday)t2
ON
t1.shop_id = t2.shop_id;
EOF
