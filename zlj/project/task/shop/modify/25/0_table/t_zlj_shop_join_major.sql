

-- 店铺表关联店铺主营业务

DROP  table  if EXISTS  t_zlj_shop_join_major;

CREATE TABLE t_zlj_shop_join_major AS
  SELECT
    t1.*,
    CASE WHEN main_cat_name IS NULL
      THEN '-'
    ELSE main_cat_name END AS main_cat_name
  FROM
    (
      SELECT *    FROM         t_base_ec_shop_dev_new
            WHERE ds = 20160615 AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
    ) t1 LEFT JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id ;