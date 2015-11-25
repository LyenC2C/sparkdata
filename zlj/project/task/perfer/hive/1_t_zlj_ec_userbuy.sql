


item_ds=$1
feed_ds=$2

/home/hadoop/hive/bin/hive<<EOF


SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;


DROP TABLE IF EXISTS t_zlj_ec_userbuy;
CREATE TABLE t_zlj_ec_userbuy
  AS
    SELECT
      t1.*,
      t2.user_id,
      t2.f_date,

      round(log10(cast(t1.price as FLOAT))*pow(0.5, log2(datediff)/4.0)*10,4) AS score
    FROM
      (

        SELECT
          item_id,
          cat_id,
          root_cat_id,
          root_cat_name,
          brand_id,
          brand_name,
          cast(price AS INT) price,
          shop_id
        FROM t_base_ec_item_dev
        WHERE ds = '$item_ds'
      ) t1
      JOIN
      (
        SELECT
          item_id,
          user_id,
          f_date,
--           datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), f_date) - 40 AS datediff
          datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), f_date)  AS datediff
        FROM
          t_base_ec_item_feed_dev
        WHERE ds > '$feed_ds' and user_id rlike   '^\\\\d+$'  and item_id rlike   '^\\\\d+$'

      ) t2 ON t1.item_id = t2.item_id
 ;


  EOF