





set hive.exec.reducers.bytes.per.reducer=500000000;
use wlbase_dev;


LOAD DATA  INPATH '/hive/external/wlbase_dev/t_base_ec_shop_dev/ds=20150101' OVERWRITE INTO TABLE t_base_ec_shop_dev PARTITION (ds='20150000');



insert  OVERWRITE table t_base_ec_shop_dev PARTITION(ds='20151024')
select
shop_id       ,
seller_id     ,
shop_name     ,
seller_name   ,
star          ,
credit        ,
starts        ,
bc_type       ,
item_count    ,
fans_count    ,
good_rate_p   ,
weitao_id     ,
desc_score    ,
service_score ,
wuliu_score   ,
ts
from
(
select
shop_id       ,
seller_id     ,
shop_name     ,
seller_name   ,
star          ,
credit        ,
starts        ,
bc_type       ,
cast(item_count as int ) item_count  ,
cast(fans_count  as int )  fans_count ,
cast( regexp_replace(good_rate_p,'%','')  as FLOAT ) good_rate_p,
weitao_id     ,
cast(desc_score    as FLOAT )desc_score,
cast(service_score as FLOAT )service_score,
cast(wuliu_score   as FLOAT )wuliu_score,
ts       ,
ROW_NUMBER() OVER(PARTITION BY shop_id ORDER BY seller_id desc) AS rn
from
t_zlj_t_base_ec_shop_dev

  where ds=20150000

)t where  rn=1  ;
