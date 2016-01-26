
drop table if EXISTS  t_zlj_brand_feed_tags;

CREATE TABLE t_zlj_brand_feed_tags
  AS
    SELECT
      brand_id,brand_name ,
      concat_ws(' ',collect_set(concat_ws('_', word, CAST(num AS STRING)))) AS feed_tags
    FROM
      (
        select brand_id,brand_name ,word,num,ROW_NUMBER() OVER(PARTITION BY brand_id  ORDER BY num DESC) AS rn
        from

          (
          SELECT
             brand_id,brand_name ,
             word,
             count(1) AS num
           FROM
             (
               SELECT
                 brand_id,brand_name ,
                 word
               FROM
                 (
                   SELECT
                     brand_id,brand_name ,
                     impr_c
                   FROM
                     t_zlj_feed2015_parse_v3_jion_cat_brand_shop
                   WHERE LENGTH(impr_c) > 1  and LENGTH(brand_id)>0
                 ) t5
               LATERAL  VIEW explode(split(impr_c, '\\|'))t1 AS word
             ) t
           GROUP BY brand_id,brand_name , word
          )t3
      ) t1

      where rn <20
    GROUP BY brand_id,brand_name  ;
