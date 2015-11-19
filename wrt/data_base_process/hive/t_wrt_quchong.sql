ds=$1
/home/hadoop/hive/bin/hive<<EOF
use wlbase_dev;
INSERT OVERWRITE TABLE t_base_ec_item_sale_dev PARTITION(ds=$ds)
SELECT
  item_id,
  item_title,
  r_price,
  s_price,
  bc_type,
  quantity,
  total_sold,
  order_cost,
  shop_id,
  ts
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY ts DESC) AS rn
    FROM t_base_ec_item_sale_dev
    where ds=$ds
  )y
WHERE rn = 1