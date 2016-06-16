

drop table  t_zlj_shop_anay_statis_info_all ;

create table t_zlj_shop_anay_statis_info_all as
SELECT
  t3.shop_id,
  t3.shop_name,
  sum(r.totsnu)   tsnu,
  sum(r.totsmo)   tsmo,
  sum(r.quantity) trenu,
  sum(r.restmo)   tremo
FROM
  (
    SELECT
      t1.item_id,
      t1.totsnu,
      t1.totsmo,
      t2.quantity,
      t2.restmo
    FROM
      (SELECT
         item_id,
         sum(day_sold) totsnu,
         sum(daysmo)   totsmo
       FROM
         (SELECT
            item_id,
            day_sold,
            day_sold_price daysmo
          FROM t_base_ec_item_daysale_dev_new  where (day_sold_price/day_sold)<50000) t
       GROUP BY item_id) t1
      JOIN
      (SELECT
         item_id,
         qu quantity,
         cast(price * qu AS FLOAT) restmo
       FROM t_base_ec_item_sold_dev
       WHERE ds = '20160528' and price<100000) t2
        ON t1.item_id = t2.item_id
  ) r
  JOIN
  (SELECT
     y1.item_id,
     y1.shop_id,
     y2.shop_name
   FROM (SELECT
           item_id,
           shop_id
         FROM t_base_ec_item_dev_new
         WHERE ds = '20160607') y1
     JOIN
     (SELECT
        shop_id,
        shop_name
      FROM t_base_ec_shop_dev
      WHERE ds = '20160613'
     ) y2
       ON y1.shop_id = y2.shop_id
  ) t3
    ON r.item_id = t3.item_id
GROUP BY shop_id,shop_name  ;