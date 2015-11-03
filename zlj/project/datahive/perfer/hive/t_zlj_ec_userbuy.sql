
set hive.exec.reducers.bytes.per.reducer=500000000;
use wlbase_dev;


-- 计算出分数
drop table IF EXISTS  t_zlj_ec_userbuy;
create table t_zlj_ec_userbuy as
select t1.* ,t2.user_id,t2.f_date,

  pow(0.5,datediff/4.0) as score
  from
(

select
  item_id,
  cat_id,
  root_cat_id   ,
  brand_id,
  cast(price as int) price,
  shop_id
FROM t_base_ec_item_dev
  where ds=20151030

)t1
join
(
SELECT

item_id,
user_id,
f_date ,
datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),f_date)-40 as datediff
from
t_base_ec_item_feed_dev
where ds >20150701

)t2 on t1.item_id=t2.item_id
