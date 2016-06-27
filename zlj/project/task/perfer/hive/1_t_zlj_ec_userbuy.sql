item_ds=$1
feed_ds=$2

/home/zlj/hive/bin/hive<<EOF


SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;


DROP TABLE IF EXISTS t_zlj_ec_userbuy_info;
CREATE TABLE t_zlj_ec_userbuy_info
  AS
    SELECT
      item_id,
      cat_id,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      cast(price AS DOUBLE)       price,
      user_id,
      shop_id,

      round(log(1.3,price+1 ) * pow(2.8, (datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),
                                                            concat_ws('-', substring(ds, 1, 4), substring(ds, 5, 2),
                                                                      substring(ds, 7, 2)))) *(-0.005)) , 4)+1 AS score

    FROM
      t_base_ec_record_dev_new

    WHERE CAST(item_id AS INT) > 0 AND CAST(cat_id AS INT) > 0 AND CAST(user_id AS INT) > 0;


DROP TABLE IF EXISTS t_zlj_ec_userbuy;

create table  t_zlj_ec_userbuy as
SELECT
  t1.*,
  t2.sum_score
FROM t_zlj_ec_userbuy_info t1 JOIN
  (
    SELECT
      user_id,
      sum(score) AS sum_score
    FROM t_zlj_ec_userbuy_info
    GROUP BY user_id

  ) t2 ON t1.user_id = t2.user_id ;

EOF





