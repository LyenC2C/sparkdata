


/home/zlj/hive/bin/hive<<EOF


SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_perfer_tag_data;
CREATE TABLE t_zlj_ec_perfer_tag_data
AS

 SELECT
  user_id,
  cate_level2_id,
  price
 FROM
  (
   SELECT
    user_id,
    cate_level2_id,
    price,
    ROW_NUMBER()
    OVER (PARTITION BY user_id
     ORDER BY price DESC) AS rn
   FROM
    (
     SELECT
      user_id,
      cate_level2_id,
      sum(price) AS price
     FROM t_base_ec_dim AS t1 JOIN t_zlj_ec_userbuy AS t2 ON t2.cat_id = t1.cate_id
     WHERE cate_level2_id IS NOT NULL
     GROUP BY user_id, cate_level2_id

    ) t

  ) t1
 WHERE rn < 15;

EOF

spark-submit  --total-executor-cores  100   --executor-memory  15g  --driver-memory 15g   ../spark/user_cat_tag_v2.py