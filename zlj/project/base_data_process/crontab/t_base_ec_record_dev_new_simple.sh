
/home/zlj/hive/bin/hive<<EOF

use wlbase_dev;


DROP  table t_base_ec_record_dev_new_simple  ;


create table t_base_ec_record_dev_new_simple as
SELECT

item_id          ,
user_id          ,
content_length   ,
annoy            ,
dsn               ,
datediff         ,
cat_id           ,
root_cat_id      ,
brand_id         ,
bc_type          ,
price            ,
shop_id          ,
ds
from t_base_ec_record_dev_new
where ds='true'
;



EOF



20161017   11106898423

