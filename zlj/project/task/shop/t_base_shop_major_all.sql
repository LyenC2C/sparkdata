
CREATE TABLE t_base_shop_major_all AS SELECT
                                        shop_id,
                                        shop_name,
                                        industry_name AS main_cat_name
                                      FROM t_lzh_shop_dim
                                      ;

                                      WHERE LENGTH(industry_name) > 2;



CREATE TABLE t_base_shop_major_all AS


SELECT q.*
FROM (
       SELECT
         p.shop_id,p.shop_name,p.industry_name,rank() over(partition BY p.shop_id ORDER BY shopsalmon DESC) rk
       FROM
         (
           SELECT
             o.shop_id,
             o.shop_name,
             o.industry_name,
             sum(o.totsmo) shopsalmon
           FROM
             (
               SELECT
                 i1.*,
                 i2.industry_name
               FROM
                 (
                   SELECT
                     u1.*,
                     u2.cat_id
                   FROM
                     (
                       SELECT
                         t1.item_id,
                         t1.totsnu,
                         t1.totsmo,
                         t3.shop_id,
                         t3.shop_name
                       FROM
                         (SELECT
                            item_id,
                            sum(day_sold) totsnu,
                            sum(daysmo)   totsmo
                          FROM
                            (SELECT
                               item_id,
                               day_sold,
                               day_sold_price daysmo
                             FROM t_base_ec_item_daysale_dev_new) t
                          GROUP BY item_id) t1
                         JOIN
                         (SELECT
                            y1.item_id,
                            y1.shop_id,
                            y2.shop_name
                          FROM (SELECT
                                  item_id,
                                  shop_id
                                FROM t_base_ec_item_dev_new
                                WHERE ds = '20160607') y1
                            JOIN
                            (SELECT
                               shop_id,
                               shop_name
                             FROM t_base_ec_shop_dev
                             WHERE ds = '20160613'
                            ) y2
                              ON y1.shop_id = y2.shop_id
                         ) t3
                           ON t1.item_id = t3.item_id
                     ) u1
                     JOIN
                     (SELECT
                        item_id,
                        cat_id
                      FROM t_base_ec_item_dev_new
                      WHERE ds = '20160613') u2
                       ON u1.item_id = u2.item_id) i1
                 JOIN
                 (SELECT
                    cate_id,
                    case when  length(industry_name)<1    then cate_level1_name else industry_name end as industry_name
                  FROM t_base_ec_dim
                  WHERE ds = '20151023') i2
                   ON i1.cat_id = i2.cate_id) o
           GROUP BY o.shop_id, o.shop_name,o.industry_name) p
     ) q
WHERE q.rk = 1
;


