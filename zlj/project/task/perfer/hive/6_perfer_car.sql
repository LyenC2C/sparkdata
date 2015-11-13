/home/hadoop/hive/bin/hive<<EOF

--������

DROP TABLE IF EXISTS t_zlj_ec_perfer_cat;
CREATE TABLE t_zlj_ec_perfer_cat
  AS
    SELECT
      user_id,
      '�г�һ��'    tag,
      sum(f) AS score
    FROM
      t_zlj_ec_perfer_dim
    WHERE root_cat_id IN
          (
            26, 50016768, 50024971, 124470006
          )
    GROUP BY user_id;
-- order by sum(f) desc
--  limit 100;
;


EOF