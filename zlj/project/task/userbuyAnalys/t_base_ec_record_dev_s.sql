

create table  t_base_ec_record_dev_s as
  select
  item_id        ,
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