
 create table t_base_ec_item_feed_dev_inc_zlj_0612 as
 SELECT
      t2.*,
      t1.title,
      cat_id,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      bc_type,
      price,
      shop_id,
      location
    FROM (SELECT
            cast(item_id AS BIGINT) item_id,
            title,
            cat_id,
            root_cat_id,
            root_cat_name,
            brand_id,
            brand_name,
            bc_type,
            price,
            shop_id,
            location
          FROM t_base_ec_item_dev_new
          WHERE ds = 20160607
          ) t1
      RIGHT  JOIN
      (
        SELECT
          cast(item_id AS BIGINT)      item_id,
          feed_id,
          user_id,
          length(content)              content_length,
          annoy,
          SUBSTRING (regexp_replace(f_date,'-',''),0,8)  as ds ,
          datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), SUBSTRING (f_date,0,10)) AS datediff,
          sku
        FROM t_base_ec_item_feed_dev
        WHERE  item_id IS NOT NULL AND f_date IS NOT NULL

      ) t2 ON t1.item_id = t2.item_id;