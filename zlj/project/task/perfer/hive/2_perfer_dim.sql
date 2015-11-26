/home/hadoop/hive/bin/hive<<EOF


SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_perfer_dim;

CREATE TABLE t_zlj_ec_perfer_dim
  AS

    SELECT
      /*+ mapjoin(t1)*/
      t2.user_id,
      t2.root_cat_id,
      t1.cate_level1_name AS root_cat_name,
      t2.f,
      t2.rn


    FROM
      (
        SELECT
          cate_level1_id,
          cate_level1_name
        FROM t_base_ec_dim
      ) t1
      JOIN
      (
        SELECT
          user_id,
          root_cat_id,

          f,

          row_number() OVER (PARTITION BY user_id ORDER BY f DESC) AS rn

        FROM
        (
        SELECT
        user_id, root_cat_id, sum(score) AS f
        FROM

        t_zlj_ec_userbuy

        GROUP BY user_id, root_cat_id
        ) t

      ) t2 ON t1.cate_level1_id = t2.root_cat_id;


EOF