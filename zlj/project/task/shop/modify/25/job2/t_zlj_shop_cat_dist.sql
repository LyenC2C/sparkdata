
-- 类目分布


CREATE  TABLE  t_zlj_shop_cat_dist as


SELECT shop_id,cat_name,count(1) as cat_item_num

from t_base_ec_item_dev_new where ds=20160615
  group by shop_id,cat_name ;
