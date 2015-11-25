/home/hadoop/hive/bin/hive<<EOF


SET hive.exec.reducers.bytes.per.reducer = 500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_perfer_shop;

CREATE TABLE t_zlj_ec_perfer_shop
  AS
    SELECT
      user_id,
      shop_id,
      shop_name,
      f,

      row_number() OVER (PARTITION BY user_id ORDER BY f DESC) AS rn

FROM
  (
  SELECT
  user_id, t1.shop_id, shop_name, sum(score) AS f
  FROM

  (
  SELECT user_id, shop_id, score FROM
  t_zlj_ec_userbuy
  )t1
  JOIN
  (
  SELECT shop_id, shop_name
  FROM t_base_ec_shop_dev
  WHERE ds=20151107
  )t2 ON t1.shop_id =t2.shop_id

  GROUP BY user_id, t1.shop_id, t2.shop_name
  ) t;


EOF
