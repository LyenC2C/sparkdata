SELECT *
FROM FROM t_base_ec_shop_dev_new
WHERE ds = 20160615 AND location LIKE '%广东%';


SELECT
  dsn,
  COUNT(1)

FROM (SELECT SUBSTRING(regexp_replace(starts, '-', ''), 0, 6) AS dsn
      FROM t_base_ec_shop_dev_new
      WHERE location LIKE '%广东%'
      GROUP BY shop_id, SUBSTRING(regexp_replace(starts, '-', ''), 0, 6)
     ) t
GROUP BY dsn;


DROP TABLE t_zlj_tmp_shop_id;
CREATE TABLE t_zlj_tmp_shop_id AS
  SELECT
    shop_id,
    bc_type,
    seller_id,
    shop_name,
    seller_name
  FROM
    (
      SELECT
        shop_id,
        bc_type,
        seller_id,
        shop_name,
        seller_name
      FROM t_base_ec_shop_dev_new
      WHERE cast(shop_id AS BIGINT) > 0
      GROUP BY shop_id, bc_type, seller_id, shop_name, seller_name

      UNION ALL
      SELECT
        shop_id,
        bc_type,
        seller_id,
        shop_name,
        seller_name
      FROM t_base_ec_shop_dev
      WHERE cast(shop_id AS BIGINT) IS NULL
      GROUP BY shop_id, bc_type, seller_id, shop_name, seller_name
    ) t
  GROUP BY shop_id, bc_type, seller_id, shop_name, seller_name;


SELECT count(1)
FROM (SELECT shop_id
      FROM t_zlj_tmp_shop_id
      WHERE cast(shop_id AS BIGINT) > 0
      GROUP BY shop_id) t;


SELECT count(1)
FROM (SELECT item_id
      FROM t_base_ec_item_dev_new
      WHERE cast(item_id AS BIGINT) IS NULL
      GROUP BY item_id) t;


SELECT
  shop_id,
  bc_type,
  seller_id,
  shop_name,
  seller_name
FROM t_base_ec_shop_dev_new
WHERE cast(shop_id AS BIGINT) IS NULL
LIMIT 10;

SELECT count(1)
FROM t_base_ec_item_dev_new
WHERE cast(item_id AS BIGINT) IS NULL;


-- 59755
SELECT count(1)
FROM t_base_ec_shop_dev
WHERE cast(shop_id AS BIGINT) IS NULL;


SELECT
  y1.*,
  y2.loc_count,
  cast(y1.shop_locbccat_count / y2.loc_count AS FLOAT) shop_locbccat_percent
FROM
  (
    SELECT
      loc,
      bc_type,
      main_cat_name,
      count(1) shop_locbccat_count
    FROM
      (
        SELECT
          *,
          substr(location, 0, 2) loc
        FROM t_zlj_shop_join_major
      ) t
    GROUP BY loc, bc_type, main_cat_name
  ) y1
  JOIN
  (
    SELECT
      loc,
      count(1) loc_count
    FROM
      (
        SELECT
          shop_id,
          bc_type,
          substr(location, 0, 2) loc
        FROM t_zlj_shop_join_major
      ) t
    GROUP BY loc
  ) y2
    ON y1.loc = y2.loc

