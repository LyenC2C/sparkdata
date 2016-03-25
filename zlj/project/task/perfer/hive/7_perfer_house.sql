

/home/zlj/hive/bin/hive<<EOF



SET hive.exec.reducers.bytes.per.reducer = 500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_perfer_house;
CREATE TABLE t_zlj_ec_perfer_house
  AS
    SELECT
      user_id,
      '有房一族'    tag,
      sum(f) AS score
    FROM
      t_zlj_ec_perfer_dim
    WHERE root_cat_id IN
          (
            27,
            50008164,
            50020332,
            50020808,
            50022649,
            50022703,
            50022987,
            50023804,
            50025881,
            122852001,
            123302001,
            124698018
          )
    GROUP BY user_id
;


EOF