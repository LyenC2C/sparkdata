

--店铺商品类目分布



DROP TABLE  IF EXISTS  t_zlj_shop_cat_dist;




CREATE TABLE t_zlj_shop_cat_dist AS

  SELECT
    shop_id,
    cat_name,
    count(1) AS cat_item_num

  FROM t_base_ec_item_dev_new
  WHERE ds = 20160615
  GROUP BY shop_id, cat_name;
