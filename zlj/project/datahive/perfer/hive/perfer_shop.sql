
set hive.exec.reducers.bytes.per.reducer=500000000;
use wlbase_dev;

drop table IF EXISTS  t_zlj_ec_perfer_shop;

create table t_zlj_ec_perfer_shop as

select  user_id, shop_id ,f ,

row_number()  OVER (PARTITION BY user_id ORDER BY f desc) as rn

from
(
select
 user_id, shop_id ,sum(score) as f
from

t_zlj_ec_userbuy

group by user_id,shop_id
) t
 ;