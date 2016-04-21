

--




SELECT

t2.*,r_price,s_price,total_sold
from
t_base_ec_item_sale_dev_new  t1

 join

 (
 select
item_id, cat_id, cat_name, root_cat_id, root_cat_name, brand_id, brand_name, bc_type, price, location, shop_id
 from t_base_ec_item_dev

 ) t2 on  t1.item_id=t2.item_id  and t1.ds=20160316 and t2.ds=20160333