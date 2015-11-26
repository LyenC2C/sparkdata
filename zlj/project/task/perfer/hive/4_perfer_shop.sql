

shop_ds=$1

/home/hadoop/hive/bin/hive<<EOF



SET hive.exec.reducers.bytes.per.reducer = 500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_perfer_shop;

CREATE TABLE t_zlj_ec_perfer_shop
  AS
    SELECT
      user_id,
      t2.shop_id,
      shop_name,
      f,

      row_number() OVER (PARTITION BY user_id ORDER BY f DESC) AS rn

FROM
  (
  SELECT shop_id, shop_name
  FROM t_base_ec_shop_dev
  WHERE ds='$shop_ds'
  )t2

JOIN
  (

  SELECT user_id, shop_id, sum(score) AS f
  FROM

  t_zlj_ec_userbuy
  GROUP BY user_id, shop_id
  )t1 ON t1.shop_id =t2.shop_id

;

EOF
