


-- Æ·ÅÆ¿â
create table  t_base_ec_brand as

select

brand_id,brand_name,cat_id,cat_name,root_cat_id,root_cat_name

from t_base_ec_item_dev where ds=20160333 and LENGTH(brand_id)>1 ;