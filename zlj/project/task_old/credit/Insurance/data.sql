


create table  t_zlj_credit_insurance_feed_data_item as
SELECT
*
from
t_base_ec_record_dev_new
where root_cat_id  in (50016350 , 50024186,120950001)

t_zlj_shop_anay_statis_info