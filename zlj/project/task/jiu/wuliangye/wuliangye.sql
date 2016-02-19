



create table  t_base_ec_record_dev_wine as
  select
  item_id        ,
  title,
feed_id        ,
user_id        ,
content_length ,
annoy          ,
ds             ,
datediff       ,
cat_id         ,
root_cat_id    ,
root_cat_name  ,
brand_id       ,
brand_name     ,
bc_type        ,
price          ,
location
  from t_base_ec_record_dev
where root_cat_id=50008141 ;




-- œ˙ €∂Ó


create table t_zlj_wine_sale_ds as

select
sum(price) as sale,
ds
from
t_base_ec_record_dev_wine
group by ds ;


create table  brand_portion as
select brand_id,brand_name,sum(price)
from
t_base_ec_record_dev_wine
group by brand_id,brand_name ;