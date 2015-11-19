use wlbase_dev;
INSERT OVERWRITE TABLE t_base_ec_item_sale_dev PARTITION(ds=20151107)
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
    where ds=20151107
  )y
WHERE rn = 1