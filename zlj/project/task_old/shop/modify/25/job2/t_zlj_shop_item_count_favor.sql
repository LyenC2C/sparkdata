-- 收藏数,商品数



DROP TABLE  IF EXISTS  t_zlj_shop_item_count_favor;


CREATE TABLE t_zlj_shop_item_count_favor AS

  SELECT
    sum(favor) as favor_sum,
    COUNT(1) AS item_num,
    shop_id,
    shop_name
  FROM t_base_ec_item_dev_new
  WHERE ds = 20160615
  GROUP BY shop_id, shop_name
;




