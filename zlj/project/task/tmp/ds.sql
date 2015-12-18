#  统计类目
SELECT
  cate_level1_name,
  root_cat_id,
  num
FROM
  (SELECT
     cate_level1_id,
     cate_level1_name
   FROM t_base_ec_dim
   GROUP BY cate_level1_id, cate_level1_name)
  t1
  JOIN
  (
    SELECT
      root_cat_id,
      count(1) AS num
    FROM
      t_zlj_userbuy_cat_count
    GROUP BY root_cat_id
  ) t2
    ON
      t1.cate_level1_id = t2.root_cat_id ;