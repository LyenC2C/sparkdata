
select t1.brand_id as brand_id ,max(t1.brand_name) as brand_name, sum(t2.total) as sold
from
(
select item_id,brand_id,brand_name from t_base_ec_item_dev_new
where ds = 20161013 and paramap['产地'] = '中国' and (root_cat_id = 50010788 or root_cat_id = 1801) and bc_type = 'B'
)t1
JOIN
(
select * from t_base_ec_item_sold_dev where ds = 20161016
)t2
ON
t1.item_id = t2.item_id
group by t1.brand_id