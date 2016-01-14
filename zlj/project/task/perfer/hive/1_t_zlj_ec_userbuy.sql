item_ds=$1
feed_ds=$2

/home/hadoop/hive/bin/hive<<EOF


SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;


DROP TABLE IF EXISTS t_zlj_ec_userbuy;
CREATE TABLE t_zlj_ec_userbuy
  AS
    SELECT
      item_id,
      cat_id,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      cast(price AS INT)         price,
      user_id,

      round(log2(cast(price AS FLOAT)) *pow(0.8, (datediff(from_unixtime(unix_timestamp(), 'yyyyMMdd'), ds)) / 10.0) * 50, 4) AS score
    FROM
      t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t;

EOF