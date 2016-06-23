--创建店铺月销量表,月销售额


DROP TABLE IF EXISTS t_wrt_shop_monsold;


CREATE TABLE t_wrt_shop_monsold AS
  SELECT
    t2.shop_id             AS shop_id,
    t1.mon                 AS mon,
    sum(t1.day_sold)       AS monsales,
    sum(t1.day_sold_price) AS monsold
  FROM
    (SELECT
       item_id,
       day_sold,
       day_sold_price,
       substr(ds, 1, 6) AS mon
     FROM wlbase_dev.t_base_ec_item_daysale_dev_new) t1
    JOIN
    (SELECT
       item_id,
       shop_id
     FROM wlbase_dev.t_base_ec_item_dev_new
     WHERE ds = 20160615
    ) t2
      ON
        t1.item_id = t2.item_id
  GROUP BY t1.mon, t2.shop_id;