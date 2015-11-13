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

  GROUP BY user_id, t1.shop_id,t2.shop_name
  ) t;


# DROP TABLE IF EXISTS t_zlj_ec_perfer_shop_groupinfo;
# CREATE TABLE t_zlj_ec_perfer_shop_groupinfo
#   AS
#     SELECT
#       user_id,
#       concat_ws('|', collect_set(v)) AS shopinfos
#     FROM
#       (
#         SELECT
#           /*+ mapjoin(t2)*/
#           t1.user_id,
#           concat_ws('_', t2.shop_id, shop_name, cast(f AS STRING), cast(rn AS STRING)) AS v
#         FROM t_base_ec_shop t2 JOIN t_zlj_ec_perfer_dim t1 ON t1.root_cat_id = t2.cate_id
#         WHERE rn < 5
#       )
#       t
#     GROUP BY user_id;