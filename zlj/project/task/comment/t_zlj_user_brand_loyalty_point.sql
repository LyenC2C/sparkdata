-- loyalty_points


drop table  if EXISTS  t_zlj_user_brand_loyalty_point;


create table t_zlj_user_brand_loyalty_point  as

SELECT
  user_id,concat_ws(' ',collect_set(concat_ws('_',brand_loyalty_t,cast(f as string)))) as brand_loyalty
FROM
  (
    SELECT
      user_id,
      sum(score) AS f,
      concat_ws(' ', collect_set(concat_ws('_', brand_id, brand_name, cast(round(score, 2) AS STRING))))  as brand_loyalty_t
    FROM
      (
        SELECT
          user_id,
          cat_id,
          brand_id,
          brand_name,
          SUM(log(price)) AS score
        FROM

          t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t
        WHERE LENGTH(brand_id) > 0
        GROUP BY user_id, cat_id, brand_id,brand_name

      ) t
    GROUP BY user_id, cat_id

  ) t1  GROUP BY  user_id ;