


/home/hadoop/hive/bin/hive<<EOF


set hive.exec.reducers.bytes.per.reducer=500000000;
use  wlbase_dev;

drop table IF EXISTS  t_zlj_ec_perfer_dim;

create table t_zlj_ec_perfer_dim as

select  user_id, root_cat_id ,root_cat_name, f ,

row_number()  OVER (PARTITION BY user_id ORDER BY f desc) as rn

from
(
select
 user_id, root_cat_id ,root_cat_name ,sum(score) as f
from

t_zlj_ec_userbuy

group by user_id,root_cat_id,root_cat_name
) t
 ;


EOF