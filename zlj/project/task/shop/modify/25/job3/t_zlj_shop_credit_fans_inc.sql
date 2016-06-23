-- 店铺信用等级 粉丝数 月均分布



DROP TABLE  IF EXISTS   t_zlj_shop_credit_fans_inc;


CREATE TABLE t_zlj_shop_credit_fans_inc AS
  SELECT
    substring(ds, 0, 6) AS t_month,
    avg(credit)         AS avg_credit,
    avg(fans_count)        avg_fans_count,
    shop_id,
    shop_name
  FROM
    t_base_ec_shop_dev
  GROUP BY substring(ds, 0, 6), shop_id, shop_name;

