


SELECT
  t1.*,
  t2.cat_name,
  t2.root_cat_name,
  price
FROM
  (SELECT *
   FROM t_base_ec_item_feed_dev
   WHERE ds > 20150901
         AND ds < 20150930
   LIMIT 10000
  ) t1
  JOIN

  (SELECT *
   FROM t_base_ec_item_dev
   WHERE ds = 20151107
  )
  t2
    ON t1.item_id = t2.item_id;