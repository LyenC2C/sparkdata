
-- 包含所有类目



create table wlbase_dev.t_base_record_cate_t as
SELECT
  t2.*,
  t3.tel_index ,
  t3.rn as tel_user_rn
FROM (

SELECT
t2.* ,
  cate_level1_id,
  cate_level2_id,
  cate_level3_id,
  cate_level4_id,
  cate_level5_id
  from
       wlbase_dev.t_base_ec_dim t1
   join
     wl_base.t_base_ec_record_dev_new t2 on t2.ds='true' and t1.ds = 20161122 and t1.cate_id =t2.cat_id
  ) t2
--   left
   join wlrefer.t_zlj_phone_rank_index t3 on t2.user_id=t3.tb_id
;

Drop table  wlbase_dev.t_base_record_cate ;
alter table wlbase_dev.t_base_record_cate_t rename to t_base_record_cate;




Drop table wlbase_dev.t_base_record_cate_simple ;
create table wlbase_dev.t_base_record_cate_simple as
select
item_id        ,
feed_id        ,
user_id        ,
content_length ,
annoy          ,
dsn            ,
datediff       ,
date_predict   ,
sku            ,
cat_id         ,
root_cat_id    ,
root_cat_name  ,
cate_level1_id ,
cate_level2_id ,
cate_level3_id ,
cate_level4_id ,
cate_level5_id ,
brand_id       ,
brand_name     ,
bc_type        ,
price          ,
shop_id        ,
location       ,
tel_index      ,
tel_user_rn    ,
ds
from wl_base.t_base_record_cate
 where tel_index is not null and tel_user_rn<4 and price<160000
and  root_cat_id is not null
and ds='true' ;


-- 过滤出闲鱼数据
drop table  wlbase_dev.t_base_record_cate_simple_xianyu  ;
create table wlbase_dev.t_base_record_cate_simple_xianyu as
SELECT t2.*  from
wlcredit.t_credit_xianyu_zhima_userinfo t1 join
wlbase_dev.t_base_record_cate_simple t2 on t1.userid=t2.user_id ;




alter table wlbase_dev.t_base_record_cate_simple_xianyu  change price price float;
alter table wlbase_dev.t_base_record_cate_simple  change price price float;
alter table wlbase_dev.t_base_record_cate  change price price float;