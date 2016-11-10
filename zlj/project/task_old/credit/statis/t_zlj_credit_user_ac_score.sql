

  DROP TABLE  IF EXISTS  t_zlj_credit_user_ac_score;

  (sum(pow(2.8, datediff* (-0.005)))+20)/75

CREATE TABLE t_zlj_credit_user_ac_score AS
  SELECT
    user_id,
    sum(score) score
  FROM
    (
      SELECT
        user_id,
        shop_id,
        pow(2.8, datediff* (-0.005)) AS score
      FROM
        t_base_ec_record_dev_new
       WHERE ds='true' and  CAST(item_id AS BIGINT) > 0 AND CAST(cat_id AS BIGINT) > 0 AND CAST(user_id AS BIGINT) > 0
    ) t
  GROUP BY user_id;


-- 活跃度数据归一化

DROP TABLE t_zlj_credit_user_ac_score_normal;
CREATE TABLE t_zlj_credit_user_ac_score_normal AS
  SELECT
    t1.user_id,
    t1.score,
    (log(1.3, t1.score) - min_score) / (max_score - min_score) AS score_normal
  FROM
    t_zlj_credit_user_ac_score t1,
    (SELECT
       max(log(1.3, score)) AS max_score,
       min(log(1.3, score)) AS min_score
     FROM t_zlj_credit_user_ac_score) t2;
