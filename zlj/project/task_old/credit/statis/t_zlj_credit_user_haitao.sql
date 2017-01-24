-- 粗略查看海淘人群

CREATE TABLE t_zlj_credit_user_haitao AS
  SELECT
    user_id,
    count(1) AS times
  FROM
    (
      SELECT item_id
      FROM
        (
          SELECT item_id
          FROM t_zlj_base_ec_item_title_cut
          WHERE title_cut LIKE '%代购%' OR title_cut LIKE '%海淘%'

          UNION ALL
          SELECT t1.item_id
          FROM
            (SELECT *
             FROM t_base_ec_item_dev_new
             WHERE ds = 20160621) t1
            JOIN
            (
              SELECT shop_id
             FROM t_base_shop_type
             WHERE shop_type ['globalgou'] = 'True' OR shop_type ['tmhk'] = 'True') t2
              ON
                t1.shop_id = t2.shop_id
        ) t
      GROUP BY item_id
    )
    t1
    JOIN
    t_base_ec_record_dev_new_simple t2 ON t1.item_id = t2.item_id
  GROUP BY user_id ;



