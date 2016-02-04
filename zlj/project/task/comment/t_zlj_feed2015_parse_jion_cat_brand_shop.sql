DROP TABLE IF EXISTS t_zlj_feed2015_parse_jion_cat_brand_shop;

CREATE TABLE t_zlj_feed2015_parse_jion_cat_brand_shop
  AS
    SELECT
      t1.*,
      cat_id,
      cat_name,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      shop_id
    FROM t_zlj_feed2015_parse_v5 t1 JOIN
      (
        SELECT
          item_id,
          cat_id,
          cat_name,
          root_cat_id,
          root_cat_name,
          brand_id,
          brand_name,
          shop_id
        FROM t_base_ec_item_dev
        WHERE ds = 20160120
      ) t2
        ON t1.item_id = t2.item_id;
