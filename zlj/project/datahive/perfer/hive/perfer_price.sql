
set hive.exec.reducers.bytes.per.reducer=500000000;
use wlbase_dev;

drop table IF EXISTS  t_zlj_ec_perfer_priceavg;

create table t_zlj_ec_perfer_priceavg as

select
 user_id ,count(1)  buytimes,sum(price) as sum_price, avg(price) as avg_price
from
t_zlj_ec_userbuy
group by user_id
  HAVING  avg(price)>0
;

-- select count(1) from
-- t_zlj_ec_perfer_priceavg ;