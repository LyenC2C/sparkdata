SELECT *
FROM FROM t_base_ec_shop_dev_new
WHERE ds = 20160615 AND location LIKE '%广东%';


-- 广东店铺 时间 地域分布
SELECT
  dsn,
  loc,
  COUNT(1) ,
  bc_type

FROM (SELECT shop_id ,SUBSTRING(regexp_replace(starts, '-', ''), 0, 4) AS dsn,
  substr(location, 3, 2)  as loc,bc_type
      FROM t_base_ec_shop_dev_new
      WHERE location LIKE '%广东%' and  ds=20160622
      GROUP BY shop_id, SUBSTRING(regexp_replace(starts, '-', ''), 0, 4) ,substr(location, 3, 2),bc_type
     ) t
  where dsn >2010
GROUP BY dsn ,loc ,bc_type;




SELECT
  dsn,

  COUNT(1) ,
  bc_type

FROM (SELECT shop_id ,SUBSTRING(regexp_replace(starts, '-', ''), 0, 4) AS dsn,
  substr(location, 3, 2)  as loc,bc_type
      FROM t_base_ec_shop_dev_new
      WHERE location LIKE '%广东%' and  ds=20160622
      GROUP BY shop_id, SUBSTRING(regexp_replace(starts, '-', ''), 0, 4) ,substr(location, 3, 2),bc_type
     ) t
  where dsn >2010
GROUP BY dsn  ,bc_type;


SELECT
  loc,
  COUNT(1) ,
  bc_type

FROM (SELECT shop_id ,SUBSTRING(regexp_replace(starts, '-', ''), 0, 4) AS dsn,
  substr(location, 3, 2)  as loc,bc_type
      FROM t_base_ec_shop_dev_new
      WHERE location LIKE '%广东%' and  ds=20160622
      GROUP BY shop_id, SUBSTRING(regexp_replace(starts, '-', ''), 0, 4) ,substr(location, 3, 2),bc_type
     ) t
  where dsn >2010
GROUP BY loc  ,bc_type
;

-- 广东店铺 时间 行业分布
SELECT main_cat_name ,dsn,count(1),bc_type
FROM
(
SELECT shop_id ,SUBSTRING(regexp_replace(starts, '-', ''), 0, 4) dsn  FROM
  t_base_ec_shop_dev_new where location LIKE '%广东%' and  ds=20160622
)t1 join
t_zlj_shop_join_major t2 on t1.shop_id=t2.shop_id and  dsn >2010

group by  main_cat_name ,dsn,bc_type
;

SELECT  count(1) from t_base_ec_shop_dev_new WHERE location LIKE '%广东%' and  ds=20160622

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