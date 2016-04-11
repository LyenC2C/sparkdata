
-- 用户购买记录
-- 包括 商品信息 用户信息   店铺基本信息


create table t_base_user_buy_info  as

select
t2.item_id        ,
t2.user_id        ,
t2.content_length ,
t2.annoy          ,
t2.ds             ,
t2.sku            ,
t2.title          ,
t2.cat_id         ,
t2.root_cat_id    ,
t2.root_cat_name  ,
t2.brand_id       ,
t2.brand_name     ,
t2.bc_type        ,
t2.price          ,
t2.shop_id        ,
t2.location  ,
t1.tgender,
t1.tage,
t1.tname,
t1.tloc

from

 (
 select
tb_id,
tgender,
tage,
tname,
tloc
from t_base_user_info_s where LENGTH(tloc)>0 and LENGTH(tgender)>0
) t1 join  t_base_ec_record_dev_new  t2  on t1.tb_id =t2.user_id ;