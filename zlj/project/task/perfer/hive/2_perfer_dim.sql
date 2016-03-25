/home/zlj/hive/bin/hive<<EOF


SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_perfer_dim;

CREATE TABLE t_zlj_ec_perfer_dim
  AS
      SELECT
        user_id,
        root_cat_id,
        root_cat_name,
        f,
        row_number() OVER (PARTITION BY user_id ORDER BY f DESC) AS rn
      FROM
      (
      SELECT
      user_id, root_cat_id,root_cat_name, round(sum(score),2) AS f
      FROM
      t_zlj_ec_userbuy

      GROUP BY user_id, root_cat_id,root_cat_name
      ) t

      ;
EOF