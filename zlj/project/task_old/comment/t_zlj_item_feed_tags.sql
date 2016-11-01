
drop table if EXISTS  t_zlj_item_feed_tags;

CREATE TABLE t_zlj_item_feed_tags
  AS
    SELECT
      item_id,
      concat_ws(' ',collect_set(concat_ws('_', word, CAST(num AS STRING)))) AS feed_tags
    FROM
      (
        select item_id,word,num,ROW_NUMBER() OVER(PARTITION BY item_id ORDER BY num DESC) AS rn
        from

          (
          SELECT
             item_id,
             word,
             count(1) AS num
           FROM
             (
               SELECT
                 item_id,
                 word
               FROM
                 (
                   SELECT
                     item_id,
                     parse_info_s as impr_c
                   FROM
                   t_zlj_feed2015_parse_jion_cat_brand_shop

                   WHERE LENGTH(parse_info_s) > 1
                 ) t5
               LATERAL  VIEW explode(split(impr_c, '\\|'))t1 AS word
             ) t
           GROUP BY item_id, word
          )t3
      ) t1

      where rn <12
    GROUP BY item_id ;
