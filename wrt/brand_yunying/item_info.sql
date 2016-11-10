
use wlservice;
create table t_wrt_tmp_brand_yunying_iteminfo as
select item_id,shop_id,item_title,sold,day_sold,salePrice,up_day,down_day
from wlbase_dev.t_base_ec_shopitem_dev where ds = 20160722;

