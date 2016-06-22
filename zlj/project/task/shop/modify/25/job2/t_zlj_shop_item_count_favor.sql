


-- 收藏数

create table t_zlj_shop_item_count_favor as

SELECT sum(favor),COUNT(1) as  item_num , shop_id ,shop_name
  FROM t_base_ec_item_dev_new
WHERE ds = 20160615 group by shop_id,shop_name    ;




