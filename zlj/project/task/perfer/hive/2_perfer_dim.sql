


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



-- drop table IF EXISTS  t_zlj_ec_perfer_dim_groupinfo;
--
--  create table t_zlj_ec_perfer_dim_groupinfo as
-- SELECT
--  user_id ,concat_ws('|', collect_set(v)) as diminfos
-- FROM
-- (
-- SELECT /*+ mapjoin(t2)*/
--  t1.user_id, concat_ws('_',t2.cate_id,cate_name,cast(f as String),cast(rn as String)) as v
-- FROM t_base_ec_dim t2 join  t_zlj_ec_perfer_dim t1 on t1.root_cat_id=t2.cate_id
-- where rn <5
-- )
-- t
-- group by user_id ;

EOF