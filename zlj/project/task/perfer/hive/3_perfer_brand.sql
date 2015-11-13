



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

-- Drop table IF EXISTS  t_zlj_ec_perfer_brand_groupinfo;
--
-- create table t_zlj_ec_perfer_brand_groupinfo as
-- SELECT
--  user_id ,concat_ws('|', collect_set(brandinfo)) as brandinfos
-- FROM
-- (
-- SELECT /*+ mapjoin(t2)*/
--  t1.user_id, concat_ws('_',t2.brand_id,brand_name,cast(rn as String)) as brandinfo
--
-- FROM t_zlj_ec_perfer_brand t1 join t_base_ec_brand t2 on t1.brand_id=t2.brand_id
--
-- where t1.rn <5
-- --  ORDER BY  t1.rn
-- )
-- t
-- group by user_id ;
