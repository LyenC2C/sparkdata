

Êý¾Ý²¹È«


insert into table t_base_ec_record_dev_new
select * from t_base_ec_record_dev_new_backfind where cat_id is not null;





insert into table t_base_ec_record_dev_new
select
t1.item_id         ,
t1.feed_id         ,
t1.user_id         ,
t1.content_length  ,
t1.annoy           ,
t1.ds              ,
t1.datediff        ,
t1.sku             ,
t2.title           ,
t2.cat_id          ,
t2.root_cat_id     ,
t2.root_cat_name   ,
t2.brand_id        ,
t2.brand_name      ,
t2.bc_type         ,
t2.price           ,
t2.shop_id         ,
t2.location
from
(
select * from
t_base_ec_record_dev_new_backfind where cat_id is null
 )t1 join
t_base_ec_item_dev_new t2 on t1.item_id=t2.item_id and t2.ds=20160607 ;

7843523346

select count(1) from t_base_ec_record_dev_new_backfind where cat_id is null
99169201