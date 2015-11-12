


ds_1=$1
ds=$2

/home/hadoop/hive/bin/hive<<EOF

use wlbase_dev;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.reducers.bytes.per.reducer=1000000000;
SET hive.exec.dynamic.partition=true;

insert overwrite table t_zlj_base_ec_item_sale_dev_day partition(ds)
SELECT
  shop_id,
  t1.item_id,
  s_price,
  CASE WHEN t2.item_id IS NULL
    THEN t1.total_sold
  ELSE t1.total_sold - t2.total_sold END             AS day_sold,
  CASE WHEN t2.item_id IS NULL
    THEN s_price * t1.total_sold
  ELSE s_price * (t1.total_sold - t2.total_sold) END AS day_sold_price,

  '$ds' as ds
FROM

  (SELECT
    shop_id,
     item_id,
     s_price,
     total_sold
   FROM t_base_ec_item_sale_dev
   WHERE ds ='$ds'
    ) t1
  LEFT JOIN

  (SELECT
     item_id,
     total_sold
   FROM t_base_ec_item_sale_dev
   WHERE ds ='$ds_1')t2
    ON t1.item_id = t2.item_id
  ;
 EOF