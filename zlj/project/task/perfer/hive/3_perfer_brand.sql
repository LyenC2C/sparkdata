



/home/hadoop/hive/bin/hive<<EOF


set hive.exec.reducers.bytes.per.reducer=500000000;
use wlbase_dev;

drop table IF EXISTS  t_zlj_ec_perfer_brand;

create table t_zlj_ec_perfer_brand as

select  user_id, brand_id ,brand_name ,f ,

row_number()  OVER (PARTITION BY user_id ORDER BY f desc) as rn

from
(
select
 user_id, brand_id ,brand_name,sum(score) as f
from

t_zlj_ec_userbuy
where  LENGTH(brand_id)>2
group by user_id,brand_id ,brand_name
) t

;


EOF