SELECT
  logp,
  count(1) AS num
FROM
  (
    SELECT
      brand_id,
      brand_name,
      CAST(log(avg(price)) AS INT) AS logp
    FROM t_base_ec_item_dev
    WHERE ds = 20160333 AND root_cat_id = '1801'
          AND CAST(brand_id AS INT) > 0
    GROUP BY brand_id, brand_name
  ) t
GROUP BY logp;


SELECT
  logp,
  count(1) AS num
FROM
  (
    SELECT
      brand_id,
      brand_name,
      CAST(log(avg(price)) AS INT) AS logp
    FROM t_base_ec_item_dev
    WHERE ds = 20160333 AND root_cat_id = '50008141'
          AND CAST(brand_id AS INT) > 0
    GROUP BY brand_id, brand_name
  ) t
GROUP BY logp;


INSERT overwrite TABLE t_base_ec_brand_level PARTITION(ds='wine_root')

SELECT *
FROM
  (
    SELECT
      *,
      ROW_NUMBER()
      OVER (PARTITION BY level_num
        ORDER BY num DESC) AS rn
    FROM
      (
        SELECT
          root_cat_id,
          brand_id,
          brand_name,
          count(1)       num,
          CASE
          WHEN CAST(log(avg(price)) AS INT) <= 3
            THEN 1
          WHEN CAST(log(avg(price)) AS INT) = 4
            THEN 2
          WHEN CAST(log(avg(price)) AS INT) = 5
            THEN 3
          WHEN CAST(log(avg(price)) AS INT) = 6
            THEN 4
          WHEN CAST(log(avg(price)) AS INT) > 6
            THEN 5
          ELSE -1 END AS level_num

        FROM t_base_ec_item_dev
        WHERE ds = 20160333 AND root_cat_id = '50008141' AND bc_type = 'B'
              AND CAST(brand_id AS INT) > 0
        GROUP BY brand_id, brand_name
      ) t

  ) t2
WHERE rn < 10;


-- 品牌等级
Drop table  t_zlj_tmp_brand_level ;

CREATE TABLE t_zlj_tmp_brand_level AS
  SELECT
    brand_id,
    t1.root_cat_id,
    brand_name,
    root_cat_name,
    CASE WHEN rn <= num * 0.1
      THEN 1
    WHEN rn > num * 0.1 AND rn <= num * 0.3
      THEN 2
    ELSE 3 END AS brand_level
  FROM

    (
      SELECT
        brand_id,
        root_cat_id,
        brand_name,
        root_cat_name,
        ROW_NUMBER()
        OVER (PARTITION BY avg_price
          ORDER BY brand_id, root_cat_id DESC) AS rn
      FROM
        (
          SELECT
            brand_id,
            root_cat_id,
            brand_name,
            root_cat_name,
            avg(price) AS avg_price
          FROM t_base_ec_item_dev_new
          WHERE ds = 20160814 AND bc_type = 'B' AND length(brand_id) > 2
          GROUP BY brand_id, root_cat_id, brand_name, root_cat_name
        ) t
    ) t1

    JOIN

    (
      SELECT
        root_cat_id,
        count(1) AS num

      FROM
        (
          SELECT
            brand_id,
            root_cat_id
          FROM t_base_ec_item_dev_new
          WHERE ds = 20160814 AND bc_type = 'B' AND length(brand_id) > 2
          GROUP BY brand_id, root_cat_id, brand_name
        ) t
      GROUP BY root_cat_id
    ) t2

      ON t1.root_cat_id = t2.root_cat_id;