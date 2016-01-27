
drop table if EXISTS  t_zlj_user_feed_tags;

CREATE TABLE t_zlj_user_feed_tags
  AS
    SELECT
      user_id,
      concat_ws(' ',collect_set(concat_ws('_', word, CAST(num AS STRING),CAST(rn AS STRING)))) AS feed_tags
    FROM
      (
        select user_id,word ,num,ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY num DESC) AS rn
        from

          (
          SELECT
             user_id,
             word,
             count(1) AS num
           FROM
             (
               SELECT
                 user_id,
                 word
               FROM
                 (
                   SELECT
                     user_id,
                     impr_c
                   FROM
                     t_zlj_feed2015_parse_v4_1
                   WHERE LENGTH(impr_c) > 1
                 ) t5
               LATERAL  VIEW explode(split(impr_c, '\\|'))t1 AS word
             ) t
           GROUP BY user_id, word
          )t3
      ) t1

      where rn <30
    GROUP BY user_id
;




