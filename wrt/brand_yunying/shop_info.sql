use wlservice;
create table t_wrt_tmp_brand_yunying_shopinfo as
select shop_id,max(shop_name)
from wlbase_dev.t_base_ec_shopitem_dev where ds = 20160722 group by shop_id;

drop table t_wrt_tmp_brand_yunying_shopinfo;

create table t_wrt_tmp_brand_yunying_shopinfo as
select shop_id,shop_name,fans_count from wlbase_dev.t_base_ec_shop_dev_new where ds = 20160731 and bc_type = 'B';

