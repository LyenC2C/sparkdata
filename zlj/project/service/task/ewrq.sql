SELECT
  ttt1.shop_id,
  ttt2.shop_name,
  ttt1.sold,
  ttt1.sales,
  ttt1.loc,
  ttt1.brand_count
FROM
  (
    SELECT
      shop_id,
      sold,
      sales,
      loc,
      brand_count
    FROM
      (
        SELECT
          tt1.shop_id,
          tt1.sold,
          tt1.sales,
          tt1.loc,
          tt2.brand_count
        FROM
          (
            SELECT
              t1.shop_id       AS shop_id,
              sum(t2.sold)     AS sold,
              sum(t2.sales)    AS sales,
              max(t1.location) AS loc
            FROM
              (SELECT
                 item_id,
                 shop_id,
                 location
               FROM t_wrt_baijiu_iteminfo) t1
              JOIN
              (SELECT
                 item_id,
                 sum(day_sold)       AS sold,
                 sum(day_sold_price) AS sales
               FROM t_wrt_baijiu_itemsale
               WHERE ds > 20160300
               GROUP BY item_id) t2
                ON
                  t1.item_id = t2.item_id
            GROUP BY
              t1.shop_id
          ) tt1
          JOIN
          (
            SELECT
              shop_id,
              count(brand_id) AS brand_count
            FROM
              (
                SELECT
                  shop_id,
                  brand_id
                FROM t_wrt_baijiu_iteminfo
                GROUP BY shop_id, brand_id
              ) t
            GROUP BY shop_id
          ) tt2
            ON
              tt1.shop_id = tt2.shop_id
      ) tt
    ORDER BY sold DESC
    LIMIT 10
  ) ttt1
  JOIN
  (SELECT
     shop_id,
     shop_name
   FROM t_wrt_baijiu_shopinfo) ttt2
    ON
      ttt1.shop_id = ttt2.shop_id;


SELECT *
FROM
  (
    SELECT

      brand_id,
      sum(1) AS    num,
      ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY num DESC) AS rn
    FROM

    (
    SELECT

    brand_id, sum(1) AS num
    FROM TABLE_name
    GROUP BY brand_id
    )t

  ) t2
WHERE rn < 6;



