


/home/zlj/hive/bin/hive<<EOF

SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_perfer_priceavg;

CREATE TABLE t_zlj_ec_perfer_priceavg
  AS

    SELECT
      user_id,
      count(1)      buytimes,
      sum(price) AS sum_price,
      round(avg(price),4) AS avg_price
    FROM
      t_zlj_ec_userbuy
    GROUP BY user_id
    HAVING avg(price) > 0;


EOF
