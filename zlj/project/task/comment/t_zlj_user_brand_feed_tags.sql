
drop table if EXISTS  t_zlj_user_brand_feed_tags;


set hive.exec.reducers.bytes.per.reducer=500000000;

CREATE TABLE t_zlj_user_brand_feed_tags
  AS
  SELECT
--       user_id ,concat_ws(' ',collect_set(concat_ws('$',brand_id,brand_name,feed_tags))) brand_feed_tags
--       from
--   (
--     SELECT
      user_id,brand_id,brand_name ,
      concat_ws(':',collect_set(concat_ws('_', word, CAST(num AS STRING)))) AS feed_tags
    FROM
      (
        select user_id,brand_id,brand_name ,word,num,ROW_NUMBER() OVER(PARTITION BY brand_id  ORDER BY num DESC) AS rn
        from

          (
          SELECT
             user_id, brand_id,brand_name ,
             word,
             count(1) AS num
           FROM
             (
               SELECT
                user_id, brand_id,brand_name ,
                 word
               FROM
                 (
                   SELECT
                    user_id, brand_id,brand_name ,
                     impr_c
                   FROM
                     t_zlj_feed2015_parse_jion_cat_brand_shop
                   WHERE LENGTH(impr_c) > 1  and LENGTH(brand_id)>2  and LENGTH(user_id)>2
                   ) t5
               LATERAL  VIEW explode(split(impr_c, '\\|'))t1 AS word
             ) t
           GROUP BY user_id,brand_id,brand_name , word
          )t3
      ) t1

      where rn <6
    GROUP BY user_id,brand_id,brand_name
--     )t4 group by user_id
    ;
