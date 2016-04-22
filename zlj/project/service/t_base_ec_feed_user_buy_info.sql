
-- 用户购买记录
-- 包括 商品信息 用户信息   店铺基本信息


-- create table t_base_ec_feed_user_buy_info  as
INSERT INTO TABLE t_base_ec_feed_user_buy_info PARTITION (ds='20160418' )

select
t2.item_id        ,
t2.user_id        ,
t2.cat_id         ,
t2.root_cat_id    ,
t2.root_cat_name  ,
t2.brand_id       ,
t2.brand_name     ,
t2.bc_type        ,
t2.price          ,
t1.tgender   ,
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
from t_base_user_info_s where  ds=20160418 and  LENGTH(tloc)>2 and LENGTH(tgender)>0  and LENGTH(tb_id)>2
) t1 join  (select item_id,
user_id        ,
cat_id         ,
root_cat_id    ,
root_cat_name  ,
brand_id       ,
brand_name     ,
bc_type        ,
price      from      t_base_ec_record_dev_new)  t2  on t1.tb_id =t2.user_id ;







