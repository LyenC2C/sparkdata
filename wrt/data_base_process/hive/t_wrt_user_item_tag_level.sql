use wlbase_dev;
create table t_wrt_user_item_tag_level as
SELECT t_zlj_ec_perfer_brand.user_id, t_zlj_ec_perfer_brand.brand_id, t_zlj_ec_perfer_brand.f,  t_wrt_item_tag_level.brand_name, t_wrt_item_tag_level.brand_tag, t_wrt_item_tag_level.brand_level
FROM t_zlj_ec_perfer_brand
JOIN t_wrt_item_tag_level ON (t_zlj_ec_perfer_brand.brand_id = t_wrt_item_tag_level.brand_id)