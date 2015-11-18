/home/hadoop/hive/bin/hive<<EOF

-- 汽车类
SET hive.exec.reducers.bytes.per.reducer = 500000000;
USE wlbase_dev;


DROP TABLE IF EXISTS t_zlj_ec_perfer_cat;
CREATE TABLE t_zlj_ec_perfer_cat
  AS
    SELECT
      user_id,
      '有车一族'    tag,
      sum(f) AS score
    FROM
      t_zlj_ec_perfer_dim
    WHERE root_cat_id IN
          (
            26, 50016768, 50024971, 124470006
          )
    GROUP BY user_id
;



EOF