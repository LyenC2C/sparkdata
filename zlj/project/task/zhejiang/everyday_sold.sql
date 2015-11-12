


ds_1=$1
ds=$2

/home/hadoop/hive/bin/hive<<EOF

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

  ds
FROM

  (SELECT
     item_id,
     s_price,
     total_sold
   FROM t_base_ec_item_sale_dev
   WHERE ds ='$ds'
    )
  LEFT JOIN

  (SELECT
     item_id,
     total_sold
   FROM t_base_ec_item_sale_dev
   WHERE ds ='$ds_1')
    ON t1.item_id = t2.item_id

 EOF