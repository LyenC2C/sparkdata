create table t_base_credit_consume_per_price as

SELECT
  user_id,
  level_,
  level_time
FROM
  (
    SELECT
      user_id,
      level_,
      level_time,
      ROW_NUMBER()
      OVER (PARTITION BY user_id
        ORDER BY level_time DESC) rn
    FROM
      (
        SELECT
          user_id,
          level_,
          count(1) level_time
        FROM
          (
            SELECT
              user_id,
              CASE WHEN log2(price) <= 2.5
                THEN 'level_1'
              WHEN log2(price) > 2.5 AND log2(price) <= 3.5
                THEN 'level_2'
              WHEN log2(price) > 3.5 AND log2(price) <= 6
                THEN 'level_3'
              WHEN log2(price) > 6 AND log2(price) <= 7.5
                THEN 'level_4'
              WHEN log2(price) > 7.5
                THEN 'level_5' END AS level_
            FROM t_base_ec_record_dev_new_simple
            WHERE price IS NOT NULL
          ) t
        GROUP BY user_id, level_
      ) t2

  ) t3
WHERE rn = 1;