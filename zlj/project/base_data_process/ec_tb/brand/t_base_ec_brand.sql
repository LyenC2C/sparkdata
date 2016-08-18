SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

use wlbase_dev;

-- insert  OVERWRITE table t_base_ec_brand PARTITION(ds)
--
-- select brand_id ,brand_name,cast(from_unixtime(unix_timestamp()-86400,'yyyyMMdd') as STRING) ds
--
--  from t_base_ec_item_dev where  ds=cast(from_unixtime(unix_timestamp()-86400,'yyyyMMdd') as STRING)
--
-- GROUP by brand_id ,brand_name;



-- insert  OVERWRITE table t_base_ec_brand PARTITION(ds)
-- select brand_id ,brand_name,cast(from_unixtime(unix_timestamp()-86400,'yyyyMMdd') as STRING) ds
--  from t_base_ec_item_dev where  ds=cast(from_unixtime(unix_timestamp()-86400,'yyyyMMdd') as STRING)
-- and  LENGTH(brand_id)>1
-- GROUP by brand_id ,brand_name;


-- insert  OVERWRITE table t_base_ec_brand PARTITION(ds='20160124')

Drop table t_base_ec_brand  ;
create table  t_base_ec_brand as
select brand_id ,brand_name,
root_cat_id,root_cat_name ,cat_id ,cat_name,
count(1)  as item_num ,
CAST (avg(price) as int ) as avg_price,
CAST ( max(price)  as int )as max_price ,
CAST ( min(price) as int ) as min_price  ,
percentile_approx(CAST(price as int),0.3 ) 03price ,
percentile_approx(CAST(price as int),0.5) 05price,
percentile_approx(CAST(price as int),0.8) 08price
 from t_base_ec_item_dev_new where  ds=20160731
and  LENGTH(brand_id)>1 and LENGTH(brand_name)>1
GROUP by brand_id ,brand_name ,root_cat_id,root_cat_name ,cat_id ,cat_name ;




select brand_id ,brand_name  from t_base_ec_item_dev where  ds=20160124 and  brand_id  rlike   '^\\d+$' limit 10 ;