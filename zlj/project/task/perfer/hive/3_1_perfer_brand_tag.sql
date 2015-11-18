


/home/hadoop/hive/bin/hive<<EOF

SET hive.exec.reducers.bytes.per.reducer = 500000000;
USE wlbase_dev;



DROP TABLE IF EXISTS t_zlj_ec_perfer_brand_groupinfo;

CREATE TABLE t_zlj_ec_perfer_brand_tag_level
  AS
    SELECT
      user_id,
      concat_ws('|', collect_set(brandtag)) AS brandtags
    FROM
      (
        SELECT
          t2.user_id,
          concat_ws('_', brand_tag, brand_level) AS brandtag
        FROM
          (
            SELECT
              brand_level,
              brand_tag
            FROM t_wrt_item_tag_level
          ) t1
          JOIN t_zlj_ec_perfer_brand t2

            ON (t2.rn < 5 AND t1.brand_id = t2.brand_id)
      ) t3
    GROUP BY user_id
;



EOF