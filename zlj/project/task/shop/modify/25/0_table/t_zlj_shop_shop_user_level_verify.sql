

-- 店铺用户购买记录 join 用户信用等级


drop  table if EXISTS  t_zlj_shop_shop_user_level_verify ;
CREATE TABLE t_zlj_shop_shop_user_level_verify AS
  SELECT
    t2.*,
    t1.*
  FROM
    (
      SELECT *
      FROM t_base_user_info_s_tbuserinfo_t
      WHERE tb_id IS NOT NULL
    ) t1
    JOIN
    (
      SELECT
        shop_id,
        user_id,
        item_id,
        ds,
        cat_id
      FROM t_base_ec_record_dev_new
      WHERE shop_id IS NOT NULL
    ) t2 ON t1.tb_id = t2.user_id;